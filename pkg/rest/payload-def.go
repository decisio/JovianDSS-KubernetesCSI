/*
Copyright (c) 2019 Open-E, Inc.
All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.
*/

package rest

///////////////////////////////////////////////////////////////////////////////
/// Error message

type ErrorT struct {
	Class   string
	Errno   int
	Message string
	Url     string
}

type ErrorData struct {
	Data  interface{}
	Error ErrorT
}

///////////////////////////////////////////////////////////////////////////////
// /pools

type IOStats struct {
	Read   string
	Write  string
	chksum string
}

type Disk struct {
	Name    string
	Id      string
	Sn      string
	Model   string
	Path    string
	Health  string
	Size    int64
	Iostats IOStats
	Led     string
	Origin  string
}

type VDevice struct {
	Name    string
	Type    string
	Health  string
	Iostats IOStats
	Disks   []Disk
}

type Enabled struct {
	Enabled bool
}

type Pool struct {
	Name       string
	Status     int
	Health     string
	Scan       int
	Operation  string
	Encryption Enabled
	Iostats    IOStats
	Vdevs      []VDevice
}

type GetPoolsData struct {
	Data  []Pool
	Error ErrorT
}

const GetPoolsRCode = 200

///////////////////////////////////////////////////////////////////////////////
/// Volume

type Volume struct {
	Origin               string
	Reference            string
	primarycache         string
	Logbias              string
	Creation             string
	Sync                 string
	Is_clone             bool
	Dedup                string
	Used                 string
	Full_name            string
	Type                 string
	Written              string
	Usedbyrefreservation string
	Compression          string
	Usedbysnapshots      string
	Copies               string
	Compressratio        string
	Readonly             string
	Mlslabel             string
	Secondarycache       string
	Available            string
	Resource_name        string
	Volblocksize         string
	Refcompressratio     string
	Snapdev              string
	Volsize              string
	Reservation          string
	Usedbychildren       string
	Usedbydataset        string
	Name                 string
	Checksum             string
	Refreservation       string
}

type GetVolumeData struct {
	Data  Volume
	Error ErrorT
}

const GetVolumeRCode = 200

type GetVolumesData struct {
	Data  []Volume `json:"data"`
	Error ErrorT   `json:"error"`
}

const GetVolumesRCode = 200

///////////////////////////////////////////////////////////////////////////////
/// Create Volume

type CreateVolume struct {
	Name string `json:"name"`
	Size string `json:"size"`
}

type CreateVolumeData struct {
	Data CreateVolumeR
}

type CreateVolumeR struct {
	Origin    string
	Is_clone  bool
	Full_name string
	Name      string
}

const CreateVolumeRCode = 201
const CreateVolumeECodeExists = 5

///////////////////////////////////////////////////////////////////////////////
/// Delete volume

type DeleteVolume struct {
	RecursivelyChildren   bool `json:"recursively_children"`
	RecursivelyDependents bool `json:"recursively_dependents"`
	ForceUmount           bool `json:"force_umount"`
}

type DeleteVolumeData struct {
	Error ErrorT
}

const DeleteVolumeRCode = 204

///////////////////////////////////////////////////////////////////////////////
/// Create Snapshot

type CreateSnapshot struct {
	Snapshot_name string `json:"snapshot_name"`
}

const CreateSnapshotRCode = 200
const CreateSnapshotECodeExists = 5

type CreateSnapshotData struct {
	Error ErrorT `json:"data"`
}

///////////////////////////////////////////////////////////////////////////////
/// Get Snapshot

type Snapshot struct {
	Referenced       string
	Name             string
	Defer_destroy    string
	Userrefs         string
	Primarycache     string
	Type             string
	Creation         string
	Refcompressratio string
	Compressratio    string
	Written          string
	Used             string
	Clones           string
	Mlslabel         string
	Secondarycache   string
}

type GetSnapshotData struct {
	Data  Snapshot
	Error ErrorT
}

const GetSnapshotRCode = 200

const GetSnapshotRCodeDNE = 500

///////////////////////////////////////////////////////////////////////////////
/// Get Snapshots

type SnapshotProperties struct {
	Creation string
}

type SnapshotShort struct {
	Volume     string
	Name       string
	Properties SnapshotProperties
}

type AllSnapshots struct {
	Results int
	Entries []SnapshotShort
}

type GetAllSnapshotsData struct {
	Data  AllSnapshots
	Error ErrorT
}

const GetAllSnapshotsRCode = 200

type VolSnapshots struct {
	Results int
	Entries []Snapshot
}

type GetVolSnapshotsData struct {
	Data  VolSnapshots
	Error ErrorT
}

const GetVolSnapshotsRCode = 200

///////////////////////////////////////////////////////////////////////////////
/// Delete Snapshot

type DeleteSnapshot struct {
	Recursively_dependents bool
}

type DeleteSnapshotData struct {
	Error ErrorT
}

const DeleteSnapshotRCode = 204
const DeleteSnapshotRCodeBusy = 1000

///////////////////////////////////////////////////////////////////////////////
/// Clone volume

type CreateClone struct {
	Name     string `json:"name"`
	Snapshot string `json:"snapshot"`
}

type CreateCloneR struct {
	Origin   string `json:"origin"`
	IsClone  bool   `json:"is_clone"`
	FullName string `json:"full_name"`
	Name     string `json:"name"`
}

type CreateCloneData struct {
	Data  CreateCloneR
	Error ErrorT
}

const CreateCloneRCode = 200

///////////////////////////////////////////////////////////////////////////////
/// Delete clone

type DeleteClone struct {
	RecursivelyChildren   bool `json:"recursively_children"`
	RecursivelyDependents bool `json:"recursively_dependents"`
	ForceUmount           bool `json:"force_umount"`
}

type DeleteCloneData struct {
	Error ErrorT
}

const DeleteCloneRCode = 200

///////////////////////////////////////////////////////////////////////////////
/// Promote cloned volume

type PromoteClone struct {
	Poolname string `json:"poolname"`
}

type PromoteCloneData struct {
	Error ErrorT
}

const PromoteCloneRCode = 200

///////////////////////////////////////////////////////////////////////////////
/// Create Target

type OutgoingUser struct {
	Password string `json:"password"`
	Name     string `json:"name"`
}

type CreateTarget struct {
	Name                string `json:"name"`
	Active              bool   `json:"active"`
	IncomingUsersActive bool   `json:"incoming_users_active"`
}

type CreateTargetData struct {
	Error ErrorT
}

const CreateTargetRCode = 201

const DeleteTargetRCode = 204

///////////////////////////////////////////////////////////////////////////////
/// Attach Volume to Target

type AttachToTarget struct {
	Name string `json:"name"`
	Lun  int    `json:"lun"`
	Mode string `json:"mode"`
}

type AttachToTargetData struct {
	Error ErrorT
}

const AttachToTargetRCode = 201
const DettachFromTargetRCode = 204

///////////////////////////////////////////////////////////////////////////////
/// Add User to Target

type AddUserToTarget struct {
	Name     string `json:"name"`
	Password string `json:"password"`
}

type AddUserToTargetData struct {
	Error ErrorT
}

const AddUserToTargetRCode = 201
