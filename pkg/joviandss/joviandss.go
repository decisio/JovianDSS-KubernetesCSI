package joviandss

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
)

var Name = "com.open-e.joviandss.csi"

// Version of plugin, should be filed during compilation
var Version string

type JovianDSS struct {
	name string
	s    *PluginServer
	cfg  *Config
	l    *logrus.Entry

	cap   []*csi.VolumeCapability_AccessMode
	cscap []*csi.ControllerServiceCapability
}

func GetPlugin(cfg *Config, l *logrus.Entry) (*JovianDSS, error) {

	j := &JovianDSS{}
	j.l = l.WithFields(logrus.Fields{
		"node": "Unknown",
		"obj":  "JovianDSS",
	})

	j.cfg = cfg
	j.cfg.DriverName = Name
	return j, nil
}

func (j *JovianDSS) Run() (err error) {
	j.l.Infof("Running %s driver, version %s", j.cfg.DriverName, Version)

	// Initialize default library driver
	j.s, err = GetPluginServer(j.cfg, j.l)
	if err != nil {
		j.l.Warn("Unable to continue Execution")
		return err
	}
	j.s.Run()
	return nil
}
