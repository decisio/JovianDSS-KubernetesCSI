package joviandss

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v2"
	"k8s.io/kubernetes/pkg/util/mount"
)

const (
	deviceIPPath = "/dev/disk/by-path/ip"
)

func (t *Target) EqualTo(stPath string) (eq bool, err error) {
	var msg string
	if exists, _ := mount.PathExists(t.TPath); exists == false {
		msg = fmt.Sprintf("Specified Target file do not exist %s", t.TPath)
		return false, status.Error(codes.Internal, msg)
	}

	var to *Target
	to, err = GetTargetFromPath(t.cfg, t.l, stPath)
	if err != nil {
		msg = fmt.Sprintf("Unable to extract Target for file: %s", t.TPath)
		t.l.Warn(msg)
		return false, status.Error(codes.InvalidArgument, msg)
	}

	if t.Tname == to.Tname {
		return true, nil
	}

	return false, nil
}

func GetTarget(cfg *NodeCfg, log *logrus.Entry, tp string) (t *Target, err error) {
	l := log.WithFields(logrus.Fields{
		"node": cfg.Id,
		"obj":  "Target",
	})
	t = &Target{
		TPath:     tp,
		TProtocol: "tcp",
	}

	t.l = l

	t.cfg = cfg
	l.Debug("Making Target")
	return t, nil

}

func GetTargetFromReq(cfg *NodeCfg, log *logrus.Entry, r interface{}) (t *Target, err error) {

	l := log.WithFields(logrus.Fields{
		"node": cfg.Id,
		"obj":  "Target",
	})

	var ctx map[string]string
	var msg string
	var vId string

	var fsType string
	var mountFlags []string

	sTPath := ""
	tPath := ""

	l.Trace("Processing request")
	if d, ok := r.(csi.NodeStageVolumeRequest); ok {

		l.Trace("Processing Stage request")
		ctx = d.GetPublishContext()
		sTPath = d.GetStagingTargetPath()
		if len(sTPath) == 0 {
			msg = fmt.Sprintf("Request do not contain StagingTargetPath.")
			l.Warn(msg)
			return nil, status.Error(codes.InvalidArgument, msg)
		}

		vId = d.GetVolumeId()
		if len(vId) == 0 {
			msg = fmt.Sprintf("Request do not contain volume id")
			l.Warn(msg)
			return nil, status.Error(codes.InvalidArgument, msg)
		}
		mount := d.GetVolumeCapability().GetMount()
		if mount != nil {
			fsType = mount.GetFsType()
			mountFlags = mount.GetMountFlags()
		}

	}

	if d, ok := r.(csi.NodePublishVolumeRequest); ok {

		l.Trace("Processing Publish request")

		ctx = d.GetPublishContext()
		tPath = d.GetTargetPath()
		if len(tPath) == 0 {
			msg = fmt.Sprintf("Request do not contain TargetPath.")
			l.Warn(msg)
			return nil, status.Error(codes.InvalidArgument, msg)
		}

		sTPath = d.GetStagingTargetPath()
		if len(sTPath) == 0 {
			msg = fmt.Sprintf("Request do not contain StagingTargetPath.")
			l.Warn(msg)
			return nil, status.Error(codes.InvalidArgument, msg)
		}

		vId = d.GetVolumeId()
		if len(vId) == 0 {
			msg = fmt.Sprintf("Request do not contain volume id")
			l.Warn(msg)
			return nil, status.Error(codes.InvalidArgument, msg)
		}
	}

	var p string
	if len(ctx["addr"]) > 0 {
		l.Tracef("Using addr from Controller")
		p = ctx["addr"]
	} else {
		l.Tracef("Using addr from Config")
		p = cfg.Addr
	}
	if len(p) == 0 {
		msg = fmt.Sprint("Unable to set storage address")
		l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	var pp string
	if len(ctx["port"]) > 0 {
		pp = ctx["port"]
	} else {
		pp = strconv.Itoa(cfg.Port)
	}
	if len(pp) == 0 {
		l.Debug("Use default port: 3260")
		pp = "3260"
	}

	iqn := ctx["iqn"]
	if len(iqn) == 0 {
		msg = fmt.Sprintf("Context do not contain iqn value")
		l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	coUser := ctx["name"]
	if len(coUser) == 0 {
		msg = fmt.Sprintf("Request do not contain CHAP name")
		l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	coPass := ctx["pass"]
	if len(coUser) == 0 {
		msg = fmt.Sprintf("Request do not contain CHAP pass")
		l.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}
	lun := ctx["lun"]
	if len(lun) == 0 {
		l.Debug("Using default lun 0")
		lun = "0"
	}

	tname := iqn + ":" + vId

	fullPortal := p + ":" + pp

	dPath := strings.Join([]string{deviceIPPath, fullPortal, "iscsi", tname, "lun", lun}, "-")

	// TODO: Provide default file system selection
	t = &Target{
		STPath:     sTPath,
		TPath:      tPath,
		DPath:      dPath,
		Portal:     p,
		PortalPort: pp,
		Iqn:        iqn,
		Tname:      vId,
		Lun:        lun,
		CoUser:     coUser, // Chap outgoing password
		CoPass:     coPass, // Chap outgoing Password
		TProtocol:  "tcp",
		FsType:     "ext3",
		MountFlags: make([]string, 0),
	}

	if len(fsType) > 0 {
		t.FsType = fsType
	}

	if len(mountFlags) > 0 {
		t.MountFlags = mountFlags
	}

	t.l = l

	t.cfg = cfg
	return t, nil
}

func GetTargetFromPath(cfg *NodeCfg, log *logrus.Entry, path string) (t *Target, err error) {

	t = &Target{}
	tp := path + "/starget"
	err = t.DeSerializeTarget(tp)
	if err != nil {
		msg := fmt.Sprintf("Unable to serialize Target file %s. Error: %s", path, err.Error())
		log.Error(msg)
		return nil, status.Error(codes.Internal, msg)
	}
	t.l = log.WithFields(logrus.Fields{
		"node": cfg.Id,
		"obj":  "Target",
	})
	t.cfg = cfg
	return t, nil
}

func (t *Target) SerializeTarget() error {

	var msg string
	d := *t
	d.CoUser = "<Cleared>"
	d.CoPass = "<Cleared>"

	data, err := yaml.Marshal(d)
	if err != nil {

		msg = fmt.Sprintf("Unable to serialize Target %+v.", d)
		return status.Error(codes.Internal, msg)
	}

	tp := t.STPath + "/starget"
	f, err := os.Create(tp)

	if err != nil {

		msg = fmt.Sprintf("Unable to create Target data file %s err %s", tp, err.Error())
		return status.Error(codes.Internal, msg)
	}

	defer f.Close()
	_, err = f.Write(data)

	if err != nil {
		msg = fmt.Sprintf("Unable to write Target data to %s err %s", tp, err.Error())
		return status.Error(codes.Internal, msg)
	}
	f.Sync()
	return nil
}

func (t *Target) DeSerializeTarget(stp string) error {
	var msg string

	data, err := ioutil.ReadFile(stp)

	if err != nil {
		msg = fmt.Sprintf("Unable to read Target data file %s err %s", stp, err)
		return status.Error(codes.Internal, msg)
	}

	err = yaml.Unmarshal(data, t)
	if err != nil {
		msg = fmt.Sprintf("Unable to deirialize Target from file  %s", stp)
		t.l.Warn(msg)
		return status.Error(codes.Internal, msg)
	}

	return nil
}

func (t *Target) DeleteSerialization() (err error) {
	var msg string
	stp := t.STPath + "/starget"
	var exists bool
	if exists, err = mount.PathExists(stp); err != nil {
		msg = fmt.Sprintf("Unable to identify serialization data for file %s. Because: %s", stp, err.Error())
		t.l.Warn(msg)
		return status.Error(codes.Internal, msg)

	}
	if exists == false {
		return nil
	}
	if err = os.Remove(t.STPath + "/starget"); err == nil {
		return nil
	}

	msg = fmt.Sprintf("Unable to delete serialized Target %s. Because: %s", stp, err.Error())
	t.l.Warn(msg)
	return err
}

func (t *Target) SetChapCred() error {

	exec := mount.NewOsExec()
	tname := t.Iqn + ":" + t.Tname

	t.l.Tracef("Target: %s", t.Tname)

	out, err := exec.Run("iscsiadm", "-m", "node", "-p", t.Portal,
		"-T", tname, "-o", "update",
		"-n", "node.session.auth.authmethod", "-v", "CHAP")
	if err != nil {
		t.l.Errorf("Could not update authentication method for %s error: %s", tname, string(out))
		return err
	}

	out, err = exec.Run("iscsiadm", "-m", "node", "-p", t.Portal,
		"-T", tname, "-o", "update",
		"-n", "node.session.auth.username", "-v", t.CoUser)
	if err != nil {
		return fmt.Errorf("iscsi: failed to update node session user error: %v", string(out))
	}
	out, err = exec.Run("iscsiadm", "-m", "node", "-p", t.Portal,
		"-T", tname, "-o", "update",
		"-n", "node.session.auth.password", "-v", t.CoPass)
	if err != nil {
		return fmt.Errorf("iscsi: failed to update node session password error: %v", string(out))
	}

	return nil
}

func (t *Target) ClearChapCred() error {

	exec := mount.NewOsExec()

	tname := t.Iqn + ":" + t.Tname

	portal := t.Portal + ":" + t.PortalPort

	exec.Run("iscsiadm", "-m", "node", "-p", portal,
		"-T", tname, "-o", "update",
		"-n", "node.session.auth.password", "-v", "")
	exec.Run("iscsiadm", "-m", "node", "-p", portal,
		"-T", tname, "-o", "update",
		"-n", "node.session.auth.username", "-v", "")

	return nil
}

func (t *Target) FormatMountVolume(req *csi.NodePublishVolumeRequest) error {
	var err error
	var msg string
	m := mount.SafeFormatAndMount{
		Interface: mount.New(""),
		Exec:      mount.NewOsExec()}

	if exists, err := mount.PathExists(t.TPath); exists == false {
		if err = os.MkdirAll(t.TPath, 0640); err != nil {
			msg = fmt.Sprintf("Unable to create directory %s, Error:%s", t.TPath, err.Error())
			return status.Error(codes.Internal, msg)

		}
	}

	fsType := req.GetVolumeCapability().GetMount().GetFsType()
	mOpt := req.GetVolumeCapability().GetMount().GetMountFlags()

	if err = m.FormatAndMount(t.DPath, t.TPath, fsType, mOpt); err != nil {
		msg = fmt.Sprintf("Unable to mount device %s, Err: %s",
			t.TPath, err.Error())
		return status.Error(codes.Internal, msg)
	}

	return nil
}

func (t *Target) UnMountVolume() error {
	var err error
	var msg string

	var exists bool

	m := mount.New("")

	devices, mCount, err := mount.GetDeviceNameFromMount(m, t.TPath)
	if err != nil {
		msg = fmt.Sprintf("Unable to get device name from mount point %s, Err: %s", t.TPath, err.Error())
		t.l.Warn(msg)
		return status.Error(codes.Internal, msg)
	}

	if exists, err = mount.PathExists(t.TPath); err != nil {
		msg = fmt.Sprintf("Target path do not exists %s, Err: %s", t.TPath, err.Error())
		t.l.Warn(msg)
		return nil

	}

	if mCount == 0 && exists == false {
		msg = fmt.Sprintf("Unable to check if target path exists %s, Err: %s", t.TPath, err.Error())
		t.l.Warn(msg)
		return status.Error(codes.Internal, msg)
	}

	if mCount > 0 {
		if err = m.Unmount(t.TPath); err != nil {
			msg = fmt.Sprintf("Unable to unmounted target %s for device %+v , Err: %s",
				t.TPath, devices, err.Error())
			t.l.Warn(msg)
			return status.Error(codes.Internal, msg)
		}
	}

	mount.CleanupMountPoint(t.TPath, m, false)

	return nil
}

//TODO: check for presence of the device
func GetStageStatus(stp string) bool {
	stp = stp + "/starget"
	if exists, _ := mount.PathExists(stp); exists == true {
		return true
	}

	return false
}

func (t *Target) StageVolume() error {

	// Scan for targets

	tname := t.Iqn + ":" + t.Tname

	fullPortal := t.Portal + ":" + t.PortalPort
	exec := mount.NewOsExec()

	devicePath := strings.Join([]string{deviceIPPath, fullPortal, "iscsi", tname, "lun", t.Lun}, "-")

	exec.Run("iscsiadm", "-m", "discoverydb", "-t", "sendtargets", "-p", t.Portal, "-o", "new")

	out, err := exec.Run("iscsiadm", "-m", "discoverydb", "-t", "sendtargets", "-p", t.Portal, "--discover")

	// Set properties

	err = t.SetChapCred()
	if err != nil {
		msg := fmt.Sprintf("iscsi: failed to update iscsi node to portal %s error: %v", tname, err)
		return errors.New(msg)
	}

	//Attach Target
	out, err = exec.Run("iscsiadm", "-m", "node", "-p", t.Portal, "-T", tname, "--login")
	if err != nil {
		t.ClearChapCred()
		exec.Run("iscsiadm", "-m", "node", "-p", t.Portal, "-T", tname, "-o", "delete")
		msg := fmt.Sprintf("iscsi: failed to attach disk: Error: %s (%v)", string(out), err)
		return errors.New(msg)
	}

	if exist := waitForPathToExist(&devicePath, 10, t.TProtocol); !exist {
		glog.Errorf("Could not attach disk to the path %s: Timeout after 10s", devicePath)
		t.ClearChapCred()
		exec.Run("iscsiadm", "-m", "node", "-p", t.Portal, "-T", tname, "-o", "delete")
		msg := "Could not attach disk: Timeout after 10s"
		return errors.New(msg)
	}

	return nil
}

func (t *Target) UnStageVolume() error {

	// Scan for targets

	var msg string
	exec := mount.NewOsExec()

	tname := t.Iqn + ":" + t.Tname

	portal := t.Portal + ":" + t.PortalPort

	if len(tname) == 0 {
		msg = fmt.Sprintf("Unable to get device target %s", tname)
		return errors.New(msg)
	}

	err := t.ClearChapCred()
	if err != nil {
		msg = fmt.Sprintf("Failed to clear ISCSI CHAP data %s error: %v", tname, err)
		return errors.New(msg)
	}

	exec.Run("iscsiadm", "-m", "node", "-p", portal, "-T", tname, "--logout")
	exec.Run("iscsiadm", "-m", "node", "-p", portal, "-T", tname, "-o", "delete")

	return nil
}

type StatFunc func(string) (os.FileInfo, error)
type GlobFunc func(string) ([]string, error)

func waitForPathToExist(devicePath *string, maxRetries int, deviceTransport string) bool {
	return waitForPathToExistInternal(devicePath, maxRetries, deviceTransport, os.Stat, filepath.Glob)
}

func waitForPathToExistInternal(devicePath *string, maxRetries int, deviceTransport string, osStat StatFunc, filepathGlob GlobFunc) bool {
	if devicePath == nil {
		return false
	}

	for i := 0; i < maxRetries; i++ {
		var err error
		if deviceTransport == "tcp" {
			_, err = osStat(*devicePath)
		} else {
			fpath, _ := filepathGlob(*devicePath)
			if fpath == nil {
				err = os.ErrNotExist
			} else {
				*devicePath = fpath[0]
			}
		}
		if err == nil {
			return true
		}
		if !os.IsNotExist(err) {
			return false
		}
		if i == maxRetries-1 {
			break
		}
		time.Sleep(time.Second)
	}
	return false
}