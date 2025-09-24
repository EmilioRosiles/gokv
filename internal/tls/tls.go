package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
)

// LoadPEM reads file
func LoadPEM(path string) ([]byte, error) {
	return os.ReadFile(path)
}

// BuildServerTLSConfig builds a tls.Config for a server that uses a cert and key and a CA pool to verify client certs optionally.
func BuildServerTLSConfig(certFile, keyFile, caCertFile string, requireClientCert bool) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if caCertFile != "" {
		caBytes, err := LoadPEM(caCertFile)
		if err != nil {
			return nil, fmt.Errorf("load ca cert: %w", err)
		}
		clientCAs := x509.NewCertPool()
		if ok := clientCAs.AppendCertsFromPEM(caBytes); !ok {
			return nil, fmt.Errorf("failed to append ca")
		}
		tlsCfg.ClientCAs = clientCAs
		if requireClientCert {
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsCfg.ClientAuth = tls.VerifyClientCertIfGiven // optional
		}
	}

	return tlsCfg, nil
}

// BuildClientTLSConfig builds a tls.Config for clients to dial servers.
// If caCertFile provided, it will use it; otherwise system roots are used.
func BuildClientTLSConfig(caCertFile, clientCertFile, clientKeyFile string, advertisedAddr string) (*tls.Config, error) {
	host, _, err := net.SplitHostPort(advertisedAddr)
	if err != nil {
		host = advertisedAddr
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: host, // important for hostname verification
	}

	if caCertFile != "" {
		caBytes, err := LoadPEM(caCertFile)
		if err != nil {
			return nil, fmt.Errorf("load ca cert: %w", err)
		}
		roots := x509.NewCertPool()
		if ok := roots.AppendCertsFromPEM(caBytes); !ok {
			return nil, fmt.Errorf("failed to append ca")
		}
		tlsCfg.RootCAs = roots
	}

	if clientCertFile != "" && clientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}
