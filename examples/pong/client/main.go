// Command client joins the local Pong server and opens a Bubble Tea TUI.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	tea "charm.land/bubbletea/v2"

	"qattidev/sgsp/examples/pong/internal/dev"
	"qattidev/sgsp/examples/pong/internal/protocol"
	"qattidev/sgsp/examples/pong/internal/tui"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "pong client:", err)
		os.Exit(1)
	}
}

func run() error {
	address := flag.String("server", protocol.Address, "server UDP address")
	dir := flag.String("dev-dir", protocol.DevDir, "local development TLS/JWT directory (same as server)")
	flag.Parse()
	material, err := dev.Load(*dir)
	if err != nil {
		return err
	}
	// Transport diagnostics must not write over Bubble Tea's alternate screen.
	logFile, err := tea.LogToFile(filepath.Join(*dir, "client.log"), fmt.Sprintf("pong client[%d]", os.Getpid()))
	if err != nil {
		return err
	}
	defer logFile.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return tui.Run(ctx, *address, material)
}
