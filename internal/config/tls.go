package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}

func GetTlsConfig(cfg TLSConfig) (*tls.Config, error) {
	config := &tls.Config{}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}

		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates[0] = cert
	}

	if CAFile != "" {
		cert, err := os.ReadFile(CAFile)
		if err != nil {
			return nil, err
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(cert) {
			return nil, fmt.Errorf("failed parsing root certificate")
		}

		if cfg.Server {
			config.ClientCAs = pool
			config.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			config.RootCAs = pool
		}
		config.ServerName = cfg.ServerAddress
	}

	return config, nil
}
