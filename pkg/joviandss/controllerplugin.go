package joviandss

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"JovianDSS-KubernetesCSI/pkg/rest"
)

const (
	kib    int64 = 1024
	mib    int64 = kib * 1024
	gib    int64 = mib * 1024
	gib100 int64 = gib * 100
	tib    int64 = gib * 1024
	tib100 int64 = tib * 100
)

const (
	minVolumeSize = 16 * mib
)

var supportedControllerCapabilities = []csi.ControllerServiceCapability_RPC_Type{
	csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
	csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
	csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
	csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,

	//TODO:
	//csi.ControllerServiceCapability_RPC_PUBLISH_READONLY,

	//csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
	//csi.ControllerServiceCapability_RPC_GET_CAPACITY,
}

var supportedVolumeCapabilities = []csi.VolumeCapability_AccessMode_Mode{
	//VolumeCapability_AccessMode_UNKNOWN,
	csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
	csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
	//VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
	//VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
	//VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,

}

type ControllerPlugin struct {
	l       *logrus.Entry
	cfg     *ControllerCfg
	iqn     string
	snapReg string

	endpoints    []*rest.StorageInterface
	capabilities []*csi.ControllerServiceCapability
	vCap         []*csi.VolumeCapability
}

func GetControllerPlugin(cfg *ControllerCfg, l *logrus.Entry) (
	cp *ControllerPlugin,
	err error) {
	cp = &ControllerPlugin{}
	lFields := logrus.Fields{
		"node":   "Controller",
		"plugin": "Controller",
	}

	cp.l = l.WithFields(lFields)

	if len(cfg.Iqn) == 0 {
		cfg.Iqn = "iqn.csi.2019-04"
	}
	cp.iqn = cfg.Iqn
	cp.cfg = cfg

	// Init Storage endpoints
	for _, s_config := range cfg.StorageEndpoints {
		var storage rest.StorageInterface
		storage, err = rest.NewProvider(&s_config, l)
		if err != nil {
			cp.l.Warnf("Creating Storage Endpoint failure %+v. Error %s",
				s_config,
				err)
			continue
		}
		cp.endpoints = append(cp.endpoints, &storage)
		cp.l.Tracef("Add Endpoint %s", s_config.Name)
	}

	if len(cp.endpoints) == 0 {
		cp.l.Warn("No Endpoints provided in config")
		return nil, errors.New("Unable to create a single endpoint")
	}

	cp.vCap = GetVolumeCapability(supportedVolumeCapabilities)

	// Init tmp volume
	cp.snapReg = cp.cfg.Nodeprefix + "SnapshotRegister"
	_, err = cp.getVolume(cp.snapReg)
	if err == nil {
		return cp, nil

	}
	vd := rest.CreateVolumeDescriptor{
		Name: cp.snapReg,
		Size: minVolumeSize,
	}
	r_err := (*cp.endpoints[0]).CreateVolume(vd)

	if r_err != nil {
		code := r_err.GetCode()
		switch code {
		case rest.RestResourceBusy:
			//According to specification from
			return nil, status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			err = status.Errorf(codes.Internal, r_err.Error())
			return nil, err

		case rest.RestObjectExists:
			cp.l.Warn("Snapshot register already exists.")

		default:
			err = status.Errorf(codes.Internal, "Unknown internal error")
			return nil, err
		}
	}

	return cp, nil
}

func (cp *ControllerPlugin) getRandomName(l int) (s string) {
	var v int64
	out := make([]byte, l)
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ01234567"

	for i := 0; i < l; i++ {
		err := binary.Read(rand.Reader, binary.BigEndian, &v)
		if err != nil {
			cp.l.Fatal(err)
		}
		out[i] = chars[v&31]
	}
	return string(out[:len(out)])
}

func (cp *ControllerPlugin) getRandomPassword(l int) (s string) {
	var v int64
	out := make([]byte, l)
	const chars = "abcdefghijklmnopqrstuvwxyz" +
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@"

	for i := 0; i < l; i++ {
		err := binary.Read(rand.Reader, binary.BigEndian, &v)
		if err != nil {
			cp.l.Fatal(err)
		}
		out[i] = chars[v&63]
	}
	return string(out[:len(out)])
}

func (cp *ControllerPlugin) getVolume(vId string) (*rest.Volume, error) {
	l := cp.l.WithFields(logrus.Fields{
		"func": "getVolume",
	})

	l.Tracef("Get volume with id: %s", vId)
	var err error

	//////////////////////////////////////////////////////////////////////////////
	/// Checks

	if len(vId) == 0 {
		msg := "Volume name missing in request"
		l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	//////////////////////////////////////////////////////////////////////////////

	v, r_err := (*cp.endpoints[0]).GetVolume(vId) // v for Volume

	if r_err != nil {
		switch r_err.GetCode() {
		case rest.RestRequestMalfunction:
			// TODO: correctly process error messages
			err = status.Error(codes.NotFound, r_err.Error())

		case rest.RestRPM:
			err = status.Error(codes.Internal, r_err.Error())
		case rest.RestResourceDNE:
			err = status.Error(codes.NotFound, r_err.Error())
		default:
			err = status.Errorf(codes.Internal, r_err.Error())
		}
		return nil, err
	}
	return v, nil

}

func (cp *ControllerPlugin) createVolumeFromSnapshot(sname string, nvname string) error {
	l := cp.l.WithFields(logrus.Fields{
		"func": "createVolumeFromSnapshot",
	})

	snameT := strings.Split(sname, "_")

	if len(snameT) != 2 {
		msg := "Unable to obtain volume name from snapshot name"
		l.Warn(msg)
		return status.Error(codes.NotFound, msg)
	}
	vname := snameT[0]

	var tmpCloneName string
	tmpCloneName = nvname[:10] + "tmpVol1"
	r_err := (*cp.endpoints[0]).CreateClone(vname, sname, tmpCloneName)
	var err error
	if r_err != nil {
		switch r_err.GetCode() {
		case rest.RestRequestMalfunction:
			// TODO: correctly process error messages
			err = status.Error(codes.NotFound, r_err.Error())
			//return nil, status.Error(codes.Internal, r_err.Error())

		case rest.RestRPM:
			err = status.Error(codes.Internal, r_err.Error())
		case rest.RestResourceDNE:
			err = status.Error(codes.NotFound, r_err.Error())
		default:
			err = status.Errorf(codes.Internal, r_err.Error())
		}
		return err
	}

	tmpSnapshot := sname[:10] + "tmpSnap1"
	r_err = (*cp.endpoints[0]).CreateSnapshot(tmpCloneName, tmpSnapshot)

	if r_err != nil {
		code := r_err.GetCode()
		switch code {
		case rest.RestResourceBusy:
			//According to specification from
			err = status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			err = status.Errorf(codes.Internal, r_err.Error())

		default:
			err = status.Errorf(codes.Internal, "Unknown internal error")

		}

		(*cp.endpoints[0]).DeleteVolume(tmpCloneName)

		(*cp.endpoints[0]).DeleteClone(vname, sname, tmpCloneName, true, true)
		return err

	}

	tmpSnapshot2 := sname[:10] + "tmpSnap2"
	r_err = (*cp.endpoints[0]).CreateSnapshot(tmpCloneName, tmpSnapshot2)

	if r_err != nil {
		code := r_err.GetCode()
		switch code {
		case rest.RestResourceBusy:
			//According to specification from
			err = status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			err = status.Errorf(codes.Internal, r_err.Error())

		default:
			err = status.Errorf(codes.Internal, "Unknown internal error")

		}
		(*cp.endpoints[0]).DeleteVolume(tmpCloneName)

		(*cp.endpoints[0]).DeleteClone(vname, sname, tmpCloneName, true, true)
		(*cp.endpoints[0]).DeleteSnapshot(tmpCloneName, tmpSnapshot)

		return err

	}

	r_err = (*cp.endpoints[0]).CreateClone(tmpCloneName, tmpSnapshot, nvname)
	if r_err != nil {
		switch r_err.GetCode() {
		case rest.RestRequestMalfunction:
			// TODO: correctly process error messages
			err = status.Error(codes.NotFound, r_err.Error())
			//return nil, status.Error(codes.Internal, r_err.Error())

		case rest.RestRPM:
			err = status.Error(codes.Internal, r_err.Error())
		case rest.RestResourceDNE:
			err = status.Error(codes.NotFound, r_err.Error())
		default:
			err = status.Errorf(codes.Internal, r_err.Error())
		}
		(*cp.endpoints[0]).DeleteSnapshot(tmpCloneName, tmpSnapshot)
		(*cp.endpoints[0]).DeleteSnapshot(tmpCloneName, tmpSnapshot2)
		(*cp.endpoints[0]).DeleteVolume(tmpCloneName)

		(*cp.endpoints[0]).DeleteClone(vname, sname, tmpCloneName, true, true)
		(*cp.endpoints[0]).DeleteSnapshot(nvname, tmpSnapshot)

		return err
	}

	r_err = (*cp.endpoints[0]).PromoteClone(tmpCloneName, tmpSnapshot, nvname)

	if r_err != nil {
		switch r_err.GetCode() {
		case rest.RestRequestMalfunction:
			// TODO: correctly process error messages
			err = status.Error(codes.NotFound, r_err.Error())
			//return nil, status.Error(codes.Internal, r_err.Error())

		case rest.RestRPM:
			err = status.Error(codes.Internal, r_err.Error())
		case rest.RestResourceDNE:
			err = status.Error(codes.NotFound, r_err.Error())
		default:
			err = status.Errorf(codes.Internal, r_err.Error())
		}
	}

	//Cleaning the mess
	(*cp.endpoints[0]).DeleteSnapshot(tmpCloneName, tmpSnapshot2)
	(*cp.endpoints[0]).DeleteVolume(tmpCloneName)
	(*cp.endpoints[0]).DeleteSnapshot(nvname, tmpSnapshot)

	return err
}

func (cp *ControllerPlugin) getVolumeSize(vname string) (int64, error) {
	l := cp.l.WithFields(logrus.Fields{
		"func": "getVolumeSize",
	})

	v, err := cp.getVolume(vname)

	if err != nil {

		msg := fmt.Sprintf("Internal error %s", err.Error())
		l.Warn(msg)
		err = status.Errorf(codes.Internal, msg)
		return 0, err
	}
	var vSize int64
	vSize, err = strconv.ParseInt((*v).Volsize, 10, 64)
	if err != nil {

		msg := fmt.Sprintf("Internal error %s", err.Error())
		l.Warn(msg)
		err = status.Errorf(codes.Internal, msg)
		return 0, err
	}

	return vSize, nil

}

func (cp *ControllerPlugin) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	cp.l.Tracef("Create volume ctx: %+v", ctx)
	var err error
	out := csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
			VolumeContext: req.GetParameters(),
		},
	}

	sourceSnapshot := ""
	sourceVolume := ""
	//////////////////////////////////////////////////////////////////////////////
	/// Checks
	if false == cp.capSupported(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME) {
		err = status.Errorf(codes.Internal, "Capability is not supported.")
		cp.l.Warnf("Unable to create volume req: %v", req)
		return nil, err
	}
	vName := req.GetName()

	if len(vName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Name missing in request")
	}

	//TODO: process volume capabilities
	caps := req.GetVolumeCapabilities()
	if caps == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}

	volumeSize := req.GetCapacityRange().GetRequiredBytes()

	if volumeSize < minVolumeSize {
		maxVSize := req.GetCapacityRange().GetLimitBytes()
		cp.l.Tracef("Minimal volume size %d too small, using max val: %d", volumeSize, maxVSize)
		volumeSize = maxVSize
	}

	if volumeSize < minVolumeSize {
		msg := fmt.Sprintf("Setting volume size to default: %d", volumeSize)
		cp.l.Warn(msg)

		volumeSize = minVolumeSize
	}

	cp.l.Tracef("Create volume %+v of size %+v",
		vName,
		volumeSize)

	//////////////////////////////////////////////////////////////////////////////

	// Get universal volume ID
	preID := []byte(cp.cfg.Salt + vName)
	rawID := sha256.Sum256(preID)
	volumeID := strings.ToLower(fmt.Sprintf("%X", rawID))

	//////////////////////////////////////////////////////////////////////////////
	// Check if volume exists
	v, err := cp.getVolume(volumeID)

	if err != nil {

		if codes.NotFound != grpc.Code(err) {
			msg := fmt.Sprintf("Internal error %s", err.Error())
			err = status.Errorf(codes.Internal, msg)
			return nil, err
		}
	}

	// Get info about volume source
	vSource := req.GetVolumeContentSource()
	if vSource != nil {
		out.Volume.ContentSource = req.GetVolumeContentSource()

		if srcSnapshot := vSource.GetSnapshot(); srcSnapshot != nil {
			sourceSnapshot = srcSnapshot.GetSnapshotId()
			// Check if snapshot exists
			_, err = cp.getSnapshot(sourceSnapshot)

			if err != nil {

				if codes.NotFound == grpc.Code(err) {
					msg := fmt.Sprintf("Snapshot specified as source does not exist %s", sourceSnapshot)

					cp.l.Warn(msg)
					return nil, status.Errorf(codes.NotFound, msg)
				}
			}

		} else {
			return nil, status.Errorf(codes.Unimplemented,
				"Unable to create volume from other sources")
		}

	}
	// TODO: develop verification of volume_capabilities, parameters

	// TODO: develop support for different max capacity
	// if voluem exists make shure it has same size
	if v != nil {
		var vSize int64
		vSize, err = strconv.ParseInt((*v).Volsize, 10, 64)
		if vSize != volumeSize {
			msg := fmt.Sprintf("Exists volume with size %d, when requsted for %d", vSize, volumeSize)
			cp.l.Warn(msg)
			err = status.Error(codes.AlreadyExists, msg)
			return nil, err
		} else {
			// Volume exists
			cp.l.Tracef("Request for the same volume %s with size %d ", volumeID, vSize)

			out.Volume.VolumeId = volumeID
			out.Volume.CapacityBytes = volumeSize

			return &out, nil
		}

	}
	//////////////////////////////////////////////////////////////////////////////
	cp.l.Tracef("req: %+v ", req)

	// Create volume

	vd := rest.CreateVolumeDescriptor{
		Name: volumeID,
		Size: volumeSize,
	}
	var r_err rest.RestError

	if len(sourceSnapshot) > 0 {
		err = cp.createVolumeFromSnapshot(sourceSnapshot, volumeID)
		if err != nil {
			return nil, err
		}
		vSize, err := cp.getVolumeSize(volumeID)
		if err != nil {
			return nil, err
		}

		out.Volume.VolumeId = volumeID
		out.Volume.CapacityBytes = vSize

		return &out, nil

	} else if len(sourceVolume) > 0 {
		return nil, status.Errorf(codes.Unimplemented, "Unable to create volume from other volume")

	} else {
		r_err = (*cp.endpoints[0]).CreateVolume(vd)
	}
	if r_err != nil {
		code := r_err.GetCode()
		switch code {
		case rest.RestResourceBusy:
			//According to specification from
			return nil, status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			err = status.Errorf(codes.Internal, r_err.Error())
			return nil, err

		case rest.RestObjectExists:
			cp.l.Warn("Specified volume already exists.")

		default:
			err = status.Errorf(codes.Internal, "Unknown internal error")
			return nil, err
		}
	}

	out.Volume.VolumeId = volumeID
	out.Volume.CapacityBytes = volumeSize

	return &out, nil

}

func (cp *ControllerPlugin) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	var err error
	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if false == cp.capSupported(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME) {
		err = status.Errorf(codes.Internal, "Capability is not supported.")
		cp.l.Warnf("Unable to delete volume req: %v", req)
		return nil, err
	}

	volumeID := req.VolumeId
	cp.l.Tracef("Deleting volume %s", volumeID)

	if r_err := (*cp.endpoints[0]).DeleteVolume(volumeID); r_err != nil {

		switch code := r_err.GetCode(); code {
		case rest.RestResourceBusy:
			//According to specification from
			return nil, status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestResourceDNE:
			return &csi.DeleteVolumeResponse{}, nil
		case rest.RestFailureUnknown:
			err = status.Errorf(codes.Internal, r_err.Error())
		default:
			err = status.Errorf(codes.Internal, "Unknown internal error")
		}

		return nil, err
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cp *ControllerPlugin) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {

	msg := fmt.Sprintf("List Volumes %+v", req)
	cp.l.Trace(msg)

	maxEnt := int64(req.GetMaxEntries())
	sToken := req.GetStartingToken()
	////////////////////////////////////////////////////////////////////////////////////////
	// Verify arguments

	if maxEnt < 0 {
		return nil, status.Errorf(codes.Internal, "Number of Entries must not ne negative.")

	}

	if len(sToken) > 0 {
		_, err := cp.getVolume(sToken)

		if err != nil {
			return nil, status.Errorf(codes.Aborted, "%s", err.Error())
		}
	}

	//////////////////////////////////////////////////////////////////////////////

	volumes, err := (*cp.endpoints[0]).ListVolumes()

	if err != nil {
		switch err.GetCode() {
		case rest.RestUnableToConnect:
			return nil, status.Errorf(codes.Internal, "Unable to connect.")

		default:
			return nil, status.Errorf(codes.Internal, "Unable to connect.")
		}
	}

	// Just return all
	if maxEnt == 0 {
		entries := make([]*csi.ListVolumesResponse_Entry, len(volumes))
		for i, name := range volumes {
			entries[i] = &csi.ListVolumesResponse_Entry{
				Volume: &csi.Volume{VolumeId: name},
			}
		}

		return &csi.ListVolumesResponse{
			Entries: entries,
		}, nil
	}

	var iToken int64
	if len(sToken) != 0 {
		iToken, _ = strconv.ParseInt(sToken, 10, 64)
		if int64(len(volumes)) < iToken {
			iToken = 0
		}
	}

	var nextToken = ""

	if int64(len(volumes)) > iToken+maxEnt {
		nextToken = strconv.FormatInt(iToken+maxEnt, 10)
		volumes = volumes[iToken : iToken+maxEnt]

	} else if iToken+maxEnt > int64(len(volumes)) {
		volumes = volumes[iToken:]
	}

	entries := make([]*csi.ListVolumesResponse_Entry, len(volumes))

	for i, name := range volumes {

		entries[i] = &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{VolumeId: name},
		}
	}

	return &csi.ListVolumesResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil

}

func (cp *ControllerPlugin) putSnapshotRecord(sId string) error {
	r_err := (*cp.endpoints[0]).CreateSnapshot(cp.snapReg, sId)

	if r_err != nil {
		code := r_err.GetCode()
		switch code {
		case rest.RestResourceBusy:
			//According to specification from
			return status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			err := status.Errorf(codes.Internal, r_err.Error())
			return err

		case rest.RestObjectExists:
			cp.l.Warn("Specified snapshot record already exists.")
			return nil

		default:
			err := status.Errorf(codes.Internal, "Unknown internal error")
			return err
		}
	}
	return nil
}

func (cp *ControllerPlugin) getSnapshotRecordExists(sId string) bool {
	_, r_err := (*cp.endpoints[0]).GetSnapshot(cp.snapReg, sId)

	if r_err != nil {
		return false
		cp.l.Infof("Snapshot record %s DNE", sId)
	}
	cp.l.Infof("Specified snapshot %s exists.", sId)
	return true
}

func (cp *ControllerPlugin) delSnapshotRecord(sId string) error {

	r_err := (*cp.endpoints[0]).DeleteSnapshot(cp.snapReg, sId)

	if r_err != nil {
		code := r_err.GetCode()
		switch code {
		case rest.RestResourceBusy:
			//According to specification from
			return status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			err := status.Errorf(codes.Internal, r_err.Error())
			return err
		case rest.RestObjectExists:
			err := status.Errorf(codes.AlreadyExists, r_err.Error())
			return err
		case rest.RestResourceDNE:
			return nil
		default:
			err := status.Errorf(codes.Internal, "Unknown internal error")
			return err
		}
	}

	return nil
}

func (cp *ControllerPlugin) getSnapshot(sId string) (*rest.Snapshot, error) {
	l := cp.l.WithFields(logrus.Fields{
		"func": "getSnapshot",
	})

	l.Tracef("Get snapshot with id: %s", sId)
	var err error

	//////////////////////////////////////////////////////////////////////////////
	/// Checks

	if len(sId) == 0 {
		msg := "Snapshot name missing in request"
		l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	snameT := strings.Split(sId, "_")

	if len(snameT) != 2 {
		msg := "Unable to obtain volume name from snapshot name"
		l.Warn(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	//////////////////////////////////////////////////////////////////////////////

	s, r_err := (*cp.endpoints[0]).GetSnapshot(snameT[0], sId)

	if r_err != nil {
		switch r_err.GetCode() {
		case rest.RestRequestMalfunction:
			// TODO: correctly process error messages
			return nil, status.Error(codes.NotFound, r_err.Error())
			//return nil, status.Error(codes.Internal, r_err.Error())

		case rest.RestRPM:
			return nil, status.Error(codes.Internal, r_err.Error())
		case rest.RestResourceDNE:
			return nil, status.Error(codes.NotFound, r_err.Error())
		default:
			err = status.Errorf(codes.Internal, r_err.Error())
		}
		return nil, err
	}
	return s, nil
}

func (cp *ControllerPlugin) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	l := cp.l.WithFields(logrus.Fields{
		"func": "CreateSnapshot",
	})

	msg := fmt.Sprintf("Create Snapshot")
	l.Tracef(msg)
	var err error

	//////////////////////////////////////////////////////////////////////////////
	/// Checks

	if false == cp.capSupported(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT) {
		err = status.Errorf(codes.Internal, "Capability is not supported.")
		l.Warnf("Unable to create volume req: %v", req)
		return nil, err
	}

	vname := req.GetSourceVolumeId()
	if len(vname) == 0 {
		msg := "Volume name missing in request"
		l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}
	sNameRaw := req.GetName()
	// Get universal volume ID

	if len(sNameRaw) == 0 {
		msg := "Snapshot name missing in request"
		l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	//////////////////////////////////////////////////////////////////////////////

	preID := []byte(cp.cfg.Salt + sNameRaw)
	rawID := sha256.Sum256(preID)
	sId := strings.ToLower(fmt.Sprintf("%X", rawID))
	sname := fmt.Sprintf("%s_%s", vname, sId)

	bExists := cp.getSnapshotRecordExists(sId)

	if bExists == true {
		cp.l.Debugf("Snapshot record exists!")
		var lerr error
		if _, lerr = cp.getSnapshot(sname); codes.NotFound == grpc.Code(lerr) {
			return nil, status.Error(codes.AlreadyExists, "Exists.")
		}
		if lerr != nil {
			cp.l.Debugf("Err value of checking related property! %s", lerr.Error())
		}
	}

	// Check if volume exists
	//TODO: implement check if snapshot exists
	l.Debugf("Req: %+v ", req)

	// Get size of volume
	var v *rest.Volume
	v, err = cp.getVolume(vname)

	if err != nil {
		return nil, err
	}

	var vSize int64
	vSize, err = strconv.ParseInt((*v).Volsize, 10, 64)

	if err != nil {
		err = status.Errorf(codes.Internal, "Unable to extract volume size.")

	}

	r_err := (*cp.endpoints[0]).CreateSnapshot(vname, sname)

	if r_err != nil {
		code := r_err.GetCode()
		switch code {
		case rest.RestResourceBusy:
			//According to specification from
			return nil, status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			err = status.Errorf(codes.Internal, r_err.Error())
			return nil, err

		case rest.RestObjectExists:
			cp.l.Warn("Specified snapshot already exists.")

		default:
			err = status.Errorf(codes.Internal, "Unknown internal error")
			return nil, err
		}
	}
	//Make record of created snapshot
	cp.putSnapshotRecord(sId)

	var s *rest.Snapshot // s for snapshot
	s, r_err = (*cp.endpoints[0]).GetSnapshot(vname, sname)

	if r_err != nil {
		code := r_err.GetCode()
		switch code {
		case rest.RestResourceBusy:
			//According to specification from
			err = status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			err = status.Errorf(codes.Internal, r_err.Error())
		default:
			err = status.Errorf(codes.Internal, "Unknown internal error")
		}
	}

	//Snapshot created successfully
	if r_err == nil {
		layout := "2006-1-2 15:4:5"
		t, err := time.Parse(layout, s.Creation)
		if err != nil {
			msg := fmt.Sprintf("Unable to get snapshot creation time: %s", err)
			cp.l.Warn(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		creationTime := &timestamp.Timestamp{

			Seconds: t.Unix(),
		}

		rsp := csi.CreateSnapshotResponse{
			Snapshot: &csi.Snapshot{
				SnapshotId:     sname,
				SourceVolumeId: vname,
				CreationTime:   creationTime,
				ReadyToUse:     true,
				SizeBytes:      vSize,
			},
		}
		cp.l.Tracef("List snapshot resp %+v", rsp)
		return &rsp, nil

	}

	return nil, err
}

func (cp *ControllerPlugin) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	// Check arguments
	l := cp.l.WithFields(logrus.Fields{
		"func": "DeleteSnapshot",
	})

	l.Tracef("Delete Snapshot req: %+v", req)
	var err error

	//////////////////////////////////////////////////////////////////////////////
	/// Checks
	if false == cp.capSupported(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT) {
		err = status.Errorf(codes.Internal, "Capability is not supported.")
		l.Warnf("Unable to create volume req: %v", req)
		return nil, err
	}

	sname := req.GetSnapshotId()
	if len(sname) == 0 {
		msg := "Snapshot id missing in request"
		l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	snameT := strings.Split(sname, "_")

	if len(snameT) != 2 {
		msg := "Unable to obtain volume name from snapshot name"
		l.Warn(msg)
		return &csi.DeleteSnapshotResponse{}, nil
		// TODO: inspect this, according to csi-test
		//return nil, status.Error(codes.InvalidArgument, msg)
	}

	vname := snameT[0]

	//////////////////////////////////////////////////////////////////////////////

	// Clean snapshot record
	cp.delSnapshotRecord(snameT[1])

	r_err := (*cp.endpoints[0]).DeleteSnapshot(vname, sname)

	if r_err != nil {
		code := r_err.GetCode()
		switch code {
		case rest.RestResourceBusy:
			//According to specification from
			return nil, status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			err = status.Errorf(codes.Internal, r_err.Error())
			return nil, err

		case rest.RestObjectExists:
			err = status.Errorf(codes.AlreadyExists, r_err.Error())
			return nil, err

		case rest.RestResourceDNE:
			return &csi.DeleteSnapshotResponse{}, nil

		default:
			err = status.Errorf(codes.Internal, "Unknown internal error")
			return nil, err
		}
	}

	_, err = cp.getSnapshot(sname)

	if err != nil {

		if codes.NotFound == grpc.Code(err) {
			msg := fmt.Sprintf("Snapshot deleted %s", sname)

			cp.l.Trace(msg)
			return &csi.DeleteSnapshotResponse{}, nil
		}
	}

	return nil, status.Errorf(codes.Internal, "Unable to delete snapshot %s", sname)

}

func (cp *ControllerPlugin) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	l := cp.l.WithFields(logrus.Fields{
		"func": "ListSnapshots",
	})
	msg := fmt.Sprintf("List snapshots %+v", req)
	l.Tracef(msg)
	var err error

	maxEnt := int64(req.GetMaxEntries())

	////////////////////////////////////////////////////////////////////////////////////////
	// Verify arguments

	if maxEnt < 0 {
		return nil, status.Errorf(codes.Internal, "Number of Entries must not be negative.")

	}
	sToken := req.GetStartingToken()

	sname := req.GetSnapshotId()

	if len(sname) != 0 {
		s, err := cp.getSnapshot(sname)

		if err != nil {
			return &csi.ListSnapshotsResponse{
				Entries: []*csi.ListSnapshotsResponse_Entry{},
			}, nil

		}

		snameT := strings.Split(sname, "_")

		iTime, r_err := rest.GetTimeStamp(s.Creation)
		if r_err != nil {
			status.Errorf(codes.Internal, "%s", r_err.Error())
		}
		timeStamp := timestamp.Timestamp{

			Seconds: iTime,
		}

		return &csi.ListSnapshotsResponse{
			Entries: []*csi.ListSnapshotsResponse_Entry{
				{
					Snapshot: &csi.Snapshot{SnapshotId: sname,
						SourceVolumeId: snameT[0],
						CreationTime:   &timeStamp},
				},
			},
		}, nil

	}

	vname := req.GetSourceVolumeId()

	if len(vname) != 0 {
		_, err = cp.getVolume(vname)
		if err != nil {

			if codes.NotFound == grpc.Code(err) {
				msg := fmt.Sprintf("Unable to find volume %s, Err%s", vname, err.Error())
				cp.l.Warn(msg)

				return &csi.ListSnapshotsResponse{
					Entries: []*csi.ListSnapshotsResponse_Entry{},
				}, nil
				return nil, err
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		if len(sname) > 0 {
		}

	}
	l.Trace("Verification done")

	//////////////////////////////////////////////////////////////////////////////
	var r_err rest.RestError

	filter := func(s string) bool {
		snameT := strings.Split(s, "_")
		if len(snameT) != 2 {
			return false
		}
		return true
	}

	var snapshots []rest.SnapshotShort
	if len(vname) == 0 {
		snapshots, r_err = (*cp.endpoints[0]).ListAllSnapshots(filter)
	} else {
		snapshots, r_err = (*cp.endpoints[0]).ListVolumeSnapshots(vname, filter)
	}

	cp.l.Debugf("Obtained snapshots: %d", len(snapshots))
	for i, s := range snapshots {
		cp.l.Debugf("Snap %d, %s", i, s)

	}

	iToken, _ := strconv.ParseInt(sToken, 10, 64)

	if iToken > int64(len(snapshots)) {
		return &csi.ListSnapshotsResponse{
			Entries: []*csi.ListSnapshotsResponse_Entry{},
		}, nil
	}

	//TODO: case with zero snapshots
	if r_err != nil {
		switch r_err.GetCode() {
		case rest.RestUnableToConnect:
			return nil, status.Errorf(codes.Internal, "Unable to connect. Err: %s", r_err.Error())
		default:
			return nil, status.Errorf(codes.Internal, "Unidentified error: %s.", r_err.Error())
		}
	}

	var nextToken = ""

	if maxEnt != 0 || len(sToken) != 0 {
		l.Trace("Listing snapshots of particular parameters")
		if maxEnt == 0 {
			maxEnt = int64(len(snapshots))
		}
		if len(sToken) != 0 {
			iToken, _ = strconv.ParseInt(sToken, 10, 64)
			if int64(len(snapshots)) < iToken {
				iToken = 0
			}
		}

		if int64(len(snapshots)) > iToken+maxEnt {
			nextToken = strconv.FormatInt(iToken+maxEnt, 10)
			snapshots = snapshots[iToken : iToken+maxEnt]

		} else {
			snapshots = snapshots[iToken:]
		}
	}

	entries := make([]*csi.ListSnapshotsResponse_Entry, len(snapshots))

	for i, s := range snapshots {
		cp.l.Debugf("Add snap %s", s.Name)
		timeInt, _ := strconv.ParseInt(s.Properties.Creation, 10, 64)
		timeStamp := timestamp.Timestamp{

			Seconds: timeInt,
		}
		entries[i] = &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SnapshotId:     s.Name,
				SourceVolumeId: s.Volume,
				CreationTime:   &timeStamp,
			},
		}
	}

	return &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil

}

func (cp *ControllerPlugin) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	l := cp.l.WithFields(logrus.Fields{
		"func": "PublishVolume",
	})

	l.Tracef("PublishVolume")
	var err error

	//////////////////////////////////////////////////////////////////////////////
	/// Checks
	vname := req.GetVolumeId()
	if len(vname) == 0 {
		msg := "Volume id is missing in request"
		l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	if len(vname) != 64 {
		msg := fmt.Sprintf("Volume id %s is incorrect", vname)
		l.Warn(msg)
		// Get universal volume ID
		preID := []byte(cp.cfg.Salt + vname)
		rawID := sha256.Sum256(preID)
		vname = strings.ToLower(fmt.Sprintf("%X", rawID))
	}
	// TODO: verify capabiolity
	caps := req.GetVolumeCapability()
	if caps == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}

	if false == cp.capSupported(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME) {
		err = status.Errorf(codes.Internal, "Capability is not supported.")
		l.Warnf("Unable to publish volume req: %v", req)
		return nil, err
	}

	roMode := req.GetReadonly()

	// Check node prefix
	nId := req.GetNodeId()

	if len(nId) == 0 {
		msg := "Node Id must be provided"
		l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	if len(cp.cfg.Nodeprefix) > len(nId) {
		msg := "Node Id is too short"
		l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}
	if strings.HasPrefix(nId, cp.cfg.Nodeprefix) == false {
		msg := "Incorrect Node Id"
		l.Warn(msg)
		return nil, status.Error(codes.NotFound, msg)

	}
	//////////////////////////////////////////////////////////////////////////////

	// Check if volume exists
	_, err = cp.getVolume(vname)

	if err != nil {

		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Create target

	tname := fmt.Sprintf("%s:%s", cp.iqn, vname)

	r_err := (*cp.endpoints[0]).CreateTarget(tname)

	if r_err != nil {
		code := r_err.GetCode()
		switch code {
		case rest.RestResourceBusy:
			//According to specification from
			return nil, status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			err = status.Errorf(codes.Internal, r_err.Error())
			return nil, err

		case rest.RestObjectExists:
			err = status.Errorf(codes.AlreadyExists, r_err.Error())
			return nil, err
		case rest.RestResourceDNE:
			msg := fmt.Sprintf("Resource not found: %s", r_err.Error())
			err = status.Errorf(codes.Internal, msg)
			return nil, err

		default:
			err = status.Errorf(codes.Internal, "Unknown internal error")
			return nil, err
		}
	}

	// Set Password
	uname := cp.getRandomName(12)
	pass := cp.getRandomPassword(16)
	r_err = (*cp.endpoints[0]).AddUserToTarget(tname, uname, pass)

	if r_err != nil {
		code := r_err.GetCode()
		switch code {
		case rest.RestResourceBusy:
			//According to specification from
			return nil, status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			err = status.Errorf(codes.Internal, r_err.Error())
			return nil, err

		case rest.RestObjectExists:
			err = status.Errorf(codes.AlreadyExists, r_err.Error())
			return nil, err

		default:
			err = status.Errorf(codes.Internal, "Unknown internal error")
			return nil, err
		}
	}

	// Attach to target
	var mode string
	if roMode == true {
		mode = "ro"
	} else {
		mode = "wt"
	}

	r_err = (*cp.endpoints[0]).AttachToTarget(tname, vname, mode)

	if r_err != nil {
		code := r_err.GetCode()
		switch code {
		case rest.RestResourceBusy:
			//According to specification from
			return nil, status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			err = status.Errorf(codes.Internal, r_err.Error())
			return nil, err
		default:
			err = status.Errorf(codes.Internal, "Unknown internal error")
			return nil, err
		}
	}
	secrets := map[string]string{"name": uname, "pass": pass}

	secrets["iqn"] = cp.iqn
	secrets["target"] = strings.ToLower(vname)

	//TODO: add target ip
	// target port
	resp := &csi.ControllerPublishVolumeResponse{
		PublishContext: secrets,
	}
	return resp, nil
}

func (cp *ControllerPlugin) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	l := cp.l.WithFields(logrus.Fields{
		"func": "UnpublishVolume",
	})

	l.Tracef("UnpublishVolume req: %+v", req)
	var err error

	//////////////////////////////////////////////////////////////////////////////
	/// Checks
	vname := req.GetVolumeId()
	if len(vname) == 0 {
		msg := "Volume name missing in request"
		l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	//////////////////////////////////////////////////////////////////////////////

	tname := fmt.Sprintf("%s:%s", cp.iqn, vname)
	r_err := (*cp.endpoints[0]).DettachFromTarget(tname, vname)

	if r_err != nil {
		c := r_err.GetCode()
		switch c {
		case rest.RestResourceDNE:

		case rest.RestResourceBusy:
			//According to specification from
			return nil, status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			status.Errorf(codes.Internal, r_err.Error())
		default:
			status.Errorf(codes.Internal, "Unknown internal error")
		}
	}

	r_err = (*cp.endpoints[0]).DeleteTarget(tname)

	if r_err != nil {
		c := r_err.GetCode()
		switch c {
		case rest.RestResourceDNE:

		case rest.RestResourceBusy:
			//According to specification from
			return nil, status.Error(codes.FailedPrecondition, r_err.Error())
		case rest.RestFailureUnknown:
			err = status.Errorf(codes.Internal, r_err.Error())
			return nil, err

		case rest.RestObjectExists:
			err = status.Errorf(codes.AlreadyExists, r_err.Error())
			return nil, err

		default:
			err = status.Errorf(codes.Internal, "Unknown internal error")
			return nil, err
		}
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cp *ControllerPlugin) ValidateVolumeCapabilities(
	ctx context.Context,
	req *csi.ValidateVolumeCapabilitiesRequest) (
	*csi.ValidateVolumeCapabilitiesResponse, error) {

	supported := true
	vname := req.GetVolumeId()
	if len(vname) == 0 {
		msg := "Volume name missing in request"
		cp.l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	_, err := cp.getVolume(vname)

	if err != nil {

		return nil, status.Error(codes.NotFound, err.Error())
	}

	vcap := req.GetVolumeCapabilities()

	if vcap == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities where not specified")
	}

	for _, c := range vcap {
		m := c.GetAccessMode()
		pass := false
		for _, mode := range supportedVolumeCapabilities {
			if mode == m.Mode {
				pass = true
			}
		}
		if pass == false {
			supported = false
			break
		}

	}

	if supported != true {

	}

	vCtx := req.GetVolumeContext()
	if vcap == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume context where not specified")
	}

	resp := &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: cp.vCap,
			VolumeContext:      vCtx,
		},
	}

	return resp, nil

}

func (cp *ControllerPlugin) ControllerExpandVolume(ctx context.Context, in *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	out := new(csi.ControllerExpandVolumeResponse)
	return out, nil
}

func (cp *ControllerPlugin) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cp *ControllerPlugin) capSupported(c csi.ControllerServiceCapability_RPC_Type) bool {
	if c == csi.ControllerServiceCapability_RPC_UNKNOWN {
		cp.l.Warn("Unknown Capability")
		return false
	}

	for _, cap := range supportedControllerCapabilities {
		if c == cap {
			return true
		}
	}
	cp.l.Debugf("Capability %s isn't supported", c)
	return false
}

func (cp *ControllerPlugin) AddControllerServiceCapabilities(
	cl []csi.ControllerServiceCapability_RPC_Type) {
	var csc []*csi.ControllerServiceCapability

	for _, c := range cl {
		cp.l.Infof(
			"Enabling controller service capability: %v",
			c.String())
		csc = append(csc, GetControllerServiceCapability(c))
	}

	cp.capabilities = csc

	return
}

func GetVolumeCapability(vcam []csi.VolumeCapability_AccessMode_Mode) []*csi.VolumeCapability {
	var out []*csi.VolumeCapability
	for _, c := range vcam {

		vc := csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: c},
		}

		out = append(out, &vc)
	}

	return out
}

func GetControllerServiceCapability(cap csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
	return &csi.ControllerServiceCapability{
		Type: &csi.ControllerServiceCapability_Rpc{
			Rpc: &csi.ControllerServiceCapability_RPC{
				Type: cap,
			},
		},
	}
}

func (cp *ControllerPlugin) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (
	*csi.ControllerGetCapabilitiesResponse,
	error,
) {
	cp.l.WithField("func", "ControllerGetCapabilities()").Infof("request: '%+v'", req)

	var capabilities []*csi.ControllerServiceCapability
	for _, c := range supportedControllerCapabilities {
		capabilities = append(capabilities, GetControllerServiceCapability(c))
	}

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: capabilities,
	}, nil
}
