package config

import (
	"os"
)

var (
	CertificatePath    string
	CertificateKeyPath string
	Port               string
	FilesRootPath      string
)

func Load() {

	CertificateKeyPath = os.Getenv("CERTIFICATE_KEY_PATH")
	CertificatePath = os.Getenv("CERTIFICATE_PATH")

	Port = os.Getenv("APP_PORT")
	if Port == "" {
		Port = "8080"
	}

	FilesRootPath = os.Getenv("FILES_ROOT_PATH")
	if FilesRootPath == "" {
		FilesRootPath = "/opt/fileconverter/files"
	}

}
