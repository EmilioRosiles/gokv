package environment

import (
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"

	"github.com/joho/godotenv"
)

type Environment struct {
	LogLevel                  slog.Level        `env:"GOKV_LOG_LEVEL"`
	CfgPath                   string            `env:"GOKV_CONFIG_PATH"`
	NodeID                    string            `env:"GOKV_CLUSTER_NODE_ID"`
	BindAddr                  string            `env:"GOKV_CLUSTER_BIND_ADDR"`
	AdvertiseAddr             string            `env:"GOKV_CLUSTER_ADVERTISE_ADDR"`
	ClusterSeeds              map[string]string `env:"GOKV_CLUSTER_SEEDS"`
	InternalTlsCAPath         string            `env:"GOKV_INTERNAL_TLS_CA_PATH"`
	InternalTlsServerCertPath string            `env:"GOKV_INTERNAL_TLS_SERVER_CERT_PATH"`
	InternalTlsServerKeyPath  string            `env:"GOKV_INTERNAL_TLS_SERVER_KEY_PATH"`
	InternalTlsClientCertPath string            `env:"GOKV_INTERNAL_TLS_CLIENT_CERT_PATH"`
	InternalTlsClientKeyPath  string            `env:"GOKV_INTERNAL_TLS_CLIENT_KEY_PATH"`
	ExternalTlsCAPath         string            `env:"GOKV_EXTERNAL_TLS_CA_PATH"`
	ExternalTlsServerCertPath string            `env:"GOKV_EXTERNAL_TLS_SERVER_CERT_PATH"`
	ExternalTlsServerKeyPath  string            `env:"GOKV_EXTERNAL_TLS_SERVER_KEY_PATH"`
	ExternalGrpcBindAddr      string            `env:"GOKV_EXTERNAL_GRPC_BIND_ADDR"`
	ExternalGrpcAdvertiseAddr string            `env:"GOKV_EXTERNAL_GRPC_ADVERTISE_ADDR"`
	ExternalRestBindAddr      string            `env:"GOKV_EXTERNAL_REST_BIND_ADDR"`
	ExternalRestAdvertiseAddr string            `env:"GOKV_EXTERNAL_REST_ADVERTISE_ADDR"`
	PersistencePath           string            `env:"GOKV_PERSISTENCE_PATH"`
}

func Default() *Environment {
	return &Environment{
		LogLevel:             slog.LevelInfo,
		CfgPath:              "config.yml",
		NodeID:               "node1",
		BindAddr:             "0.0.0.0:7946",
		ExternalGrpcBindAddr: "0.0.0.0:50051",
		ExternalRestBindAddr: "0.0.0.0:8080",
		PersistencePath:      "tmp/snapshot",
	}
}

// LoadEnvironment loads the environment variables from the .env file and the system.
func LoadEnvironment() *Environment {
	defaults := Default()

	dotEnvValues, err := godotenv.Read()
	if err != nil {
		slog.Warn(fmt.Sprintf("environment: could not read .env file: %v", err))
	}

	mergeEnvironments(defaults, dotEnvValues)
	mergeEnvironments(defaults, getOsEnv())

	return defaults
}

func mergeEnvironments(base *Environment, overlay map[string]string) {
	val := reflect.ValueOf(base).Elem()
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		tag := typ.Field(i).Tag.Get("env")

		if val, ok := overlay[tag]; ok {
			switch field.Kind() {
			case reflect.String:
				field.SetString(val)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if field.Type() == reflect.TypeOf(slog.LevelInfo) {
					field.Set(reflect.ValueOf(logLevel(val)))
				}
			case reflect.Map:
				if field.Type() == reflect.TypeOf(map[string]string{}) {
					field.Set(reflect.ValueOf(getClusterSeeds(val)))
				}
			}
		}
	}
}

func getOsEnv() map[string]string {
	envMap := make(map[string]string)
	for _, env := range os.Environ() {
		pair := strings.SplitN(env, "=", 2)
		envMap[pair[0]] = pair[1]
	}
	return envMap
}

func getClusterSeeds(str string) map[string]string {
	seedMap := make(map[string]string, 0)
	if str == "" {
		return seedMap
	}
	clusterSeeds := strings.Split(str, ",")
	for _, entry := range clusterSeeds {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			slog.Warn(fmt.Sprintf("environment: invalid seed entry: %q", entry))
			continue
		}
		seedMap[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return seedMap
}

func logLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
