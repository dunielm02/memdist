package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile         = configFile("ca.pem")
	ServerCertFile = configFile("server.pem")
	ServerKeyFile  = configFile("server-key.pem")
	RootCertFile   = configFile("root-client.pem")
	RootKeyFile    = configFile("root-client-key.pem")
	NobodyCertFile = configFile("nobody-client.pem")
	NobodyKeyFile  = configFile("nobody-client-key.pem")
	ACLModelFile   = configFile("model.conf")
	ACLPolicyFile  = configFile("policy.csv")
)

func configFile(filename string) string {
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, filename)
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	return filepath.Join(homeDir, ".memdist", filename)
}
