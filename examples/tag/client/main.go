// Command client joins the local Tag server and opens a Bubble Tea TUI.
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

	"qattidev/sgsp/examples/tag/internal/dev"
	"qattidev/sgsp/examples/tag/internal/protocol"
	"qattidev/sgsp/examples/tag/internal/tui"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tag client:", err)
		os.Exit(1)
	}
}

func run() error {
	address := flag.String("server", protocol.Address, "server UDP address")
	dir := flag.String("dev-dir", protocol.DevDir, "local development TLS/JWT directory (same as server)")
	auto := flag.Bool("auto", false, "autonomous movement")
	flag.Parse()
	material, err := dev.Load(*dir)
	if err != nil {
		return err
	}
	// Transport diagnostics must not write over Bubble Tea's alternate screen.
	logFile, err := tea.LogToFile(filepath.Join(*dir, "client.log"), fmt.Sprintf("tag client[%d]", os.Getpid()))
	if err != nil {
		return err
	}
	defer logFile.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return tui.Run(ctx, *address, material, *auto)
}
