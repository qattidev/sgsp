// Command server hosts one authoritative, two-player Pong game.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"qattidev/sgsp/examples/pong/internal/dev"
	"qattidev/sgsp/examples/pong/internal/protocol"
	"qattidev/sgsp/examples/pong/internal/server"
)

func main() {
	if err := run(); err != nil {
		log.Printf("pong server: %v", err)
		os.Exit(1)
	}
}

func run() error {
	address := flag.String("listen", protocol.Address, "server UDP address")
	dir := flag.String("dev-dir", protocol.DevDir, "local development TLS/JWT directory")
	flag.Parse()
	// Bind before generating credentials: an accidental second server should
	// fail without disturbing the running server's development environment.
	packet, err := net.ListenPacket("udp", *address)
	if err != nil {
		return err
	}
	defer packet.Close()
	material, err := dev.Ensure(*dir)
	if err != nil {
		return err
	}
	logger := log.New(os.Stdout, "pong: ", log.Ltime)
	s, err := server.New(material, packet.LocalAddr().String(), logger)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Printf("listening on %s; start two clients using -dev-dir %s (Ctrl-C to stop)", packet.LocalAddr(), *dir)
	return s.Serve(ctx, packet)
}
