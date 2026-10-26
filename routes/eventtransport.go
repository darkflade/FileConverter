package routes

import (
	"FileConverter/config"
	transporthandlers "FileConverter/eventtransport/handlers"
	"crypto/tls"
	"log"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	webtransport "github.com/quic-go/webtransport-go"
)

func WebTransportRegister() {
	address := ":" + config.Port
	cert := config.CertificatePath
	key := config.CertificateKeyPath

	if cert == "" {
		cert = config.CertificatePath
	}
	if key == "" {
		key = config.CertificateKeyPath
	}
	if cert == "" || key == "" {
		log.Printf("WebTransport TLS certificates are not configured, skipping WebTransport server on %s", address)
		return
	}

	tlsCert, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		log.Fatalf("Failed to load WebTransport TLS certificate: %v", err)
	}

	mux := http.NewServeMux()
	h3 := &http3.Server{
		Addr:    address,
		Handler: mux,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{tlsCert},
			MinVersion:   tls.VersionTLS13,
			NextProtos:   []string{http3.NextProtoH3},
		},
		QUICConfig: &quic.Config{
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
			MaxIdleTimeout:                   time.Hour,
			KeepAlivePeriod:                  20 * time.Second,
		},
	}
	wtServer := &webtransport.Server{
		H3: h3,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	webtransport.ConfigureHTTP3Server(h3)
	log.Printf("WebTransport TLS NextProtos: %#v", h3.TLSConfig.NextProtos)
	mux.HandleFunc("/webtransport/global/register", transporthandlers.HandleWebTransport(wtServer))
	mux.HandleFunc("/webtransport/global/register/", transporthandlers.HandleWebTransport(wtServer))

	go func() {
		log.Printf("Starting WebTransport server on %s", address)
		if err := wtServer.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start WebTransport server: %v", err)
		}
	}()

}
