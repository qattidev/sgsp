// Command action is a deliberately small, runnable SGSP integration example.
// It creates development-only TLS and identity assets locally, then exercises
// an input event, inventory request/reply, and a raw stream in direct mode.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	library "github.com/golang-jwt/jwt/v5"

	"qattidev/sgsp"
	identity "qattidev/sgsp/auth/jwt"
	"qattidev/sgsp/placement"
	"qattidev/sgsp/placement/memory"
)

const (
	appID       = "sgsp-action"
	devIssuer   = "sgsp-action-dev"
	inputType   = sgsp.MessageType(10)
	inventoryID = sgsp.MessageType(11)
	streamType  = sgsp.MessageType(12)
	commandType = sgsp.MessageType(13)
	updateType  = sgsp.MessageType(14)
)

type actionInput struct {
	Tick uint64 `json:"tick"`
	Move string `json:"move"`
}
type actionCommand struct {
	OperationID string `json:"operation_id"`
	Action      string `json:"action"`
}
type actionUpdate struct {
	Tick         uint64 `json:"tick"`
	InputTick    uint64 `json:"input_tick"`
	Position     int    `json:"position"`
	Coins        int    `json:"coins"`
	SessionEpoch uint64 `json:"session_epoch"`
}
type inventoryQuery struct{}
type inventoryReply struct {
	Items []string `json:"items"`
	Coins int      `json:"coins"`
}

var (
	inputMessage     = sgsp.Message[actionInput]{ID: inputType, Codec: sgsp.JSON[actionInput]()}
	commandMessage   = sgsp.Message[actionCommand]{ID: commandType, Codec: sgsp.JSON[actionCommand]()}
	updateMessage    = sgsp.Message[actionUpdate]{ID: updateType, Codec: sgsp.JSON[actionUpdate]()}
	inventoryRequest = sgsp.Request[inventoryQuery, inventoryReply]{ID: inventoryID, Input: sgsp.JSON[inventoryQuery](), Output: sgsp.JSON[inventoryReply]()}
)

// actionGame owns simulation state. Network handlers only place parsed work
// in its bounded inbox; the tick loop validates the captured session epoch at
// the commit boundary before mutating state.
type actionGame struct {
	inbox   chan gameEnvelope
	queries chan inventoryQueryWork
}
type gameEnvelope struct {
	session   sgsp.Session
	epoch     uint64
	input     *actionInput
	command   *actionCommand
	lifecycle *sgsp.Lifecycle
}
type inventoryQueryWork struct {
	id    sgsp.SessionID
	reply chan inventoryReply
}
type actionPlayer struct {
	session   sgsp.Session
	epoch     uint64
	position  int
	inputTick uint64
	items     []string
	coins     int
}

func newActionGame() *actionGame {
	return &actionGame{inbox: make(chan gameEnvelope, 1024), queries: make(chan inventoryQueryWork, 64)}
}
func (g *actionGame) enqueue(envelope gameEnvelope) {
	select {
	case g.inbox <- envelope:
	default:
		// A real game selects a policy for a saturated simulation inbox. This
		// demo drops excess inputs rather than allowing its network callback to
		// create unbounded work.
	}
}
func (g *actionGame) inventory(ctx context.Context, session sgsp.Session) (inventoryReply, error) {
	response := make(chan inventoryReply, 1)
	work := inventoryQueryWork{id: session.ID(), reply: response}
	select {
	case g.queries <- work:
	case <-ctx.Done():
		return inventoryReply{}, ctx.Err()
	}
	select {
	case value := <-response:
		return value, nil
	case <-ctx.Done():
		return inventoryReply{}, ctx.Err()
	}
}
func (g *actionGame) run(ctx context.Context) {
	ticker := time.NewTicker(time.Second / 128)
	defer ticker.Stop()
	players := make(map[sgsp.SessionID]*actionPlayer)
	operations := make(map[sgsp.SessionID]map[string]struct{})
	var tick uint64
	for {
		select {
		case envelope := <-g.inbox:
			id := envelope.session.ID()
			if envelope.lifecycle != nil {
				switch envelope.lifecycle.Kind {
				case sgsp.Opened, sgsp.Resumed:
					if envelope.session.State() != sgsp.Active || envelope.session.Epoch() != envelope.epoch {
						continue
					}
					player := players[id]
					if player == nil {
						player = &actionPlayer{session: envelope.session, items: []string{"starter-sword", "potion"}}
						players[id] = player
					}
					player.session, player.epoch = envelope.session, envelope.epoch
				case sgsp.SessionEnded:
					delete(players, id)
					delete(operations, id)
				}
				continue
			}
			player := players[id]
			if player == nil || player.epoch != envelope.epoch || envelope.session.State() != sgsp.Active || envelope.session.Epoch() != envelope.epoch {
				continue
			}
			if envelope.input != nil {
				player.inputTick = envelope.input.Tick
				switch envelope.input.Move {
				case "left":
					player.position--
				case "right":
					player.position++
				}
			}
			if envelope.command != nil && envelope.command.OperationID != "" {
				seen := operations[id]
				if seen == nil {
					seen = make(map[string]struct{})
					operations[id] = seen
				}
				if _, duplicate := seen[envelope.command.OperationID]; !duplicate && len(seen) < 256 {
					seen[envelope.command.OperationID] = struct{}{}
					if envelope.command.Action == "collect" {
						player.coins++
					}
				}
			}
		case work := <-g.queries:
			player := players[work.id]
			if player == nil {
				work.reply <- inventoryReply{}
				continue
			}
			work.reply <- inventoryReply{Items: append([]string(nil), player.items...), Coins: player.coins}
		case <-ticker.C:
			tick++
			for _, player := range players {
				if player.session.State() != sgsp.Active || player.session.Epoch() != player.epoch {
					continue
				}
				_ = sgsp.Emit(context.Background(), player.session, updateMessage, actionUpdate{Tick: tick, InputTick: player.inputTick, Position: player.position, Coins: player.coins, SessionEpoch: player.epoch}, sgsp.SendOptions{Channel: 2, Delivery: sgsp.UnreliableSequenced})
			}
		case <-ctx.Done():
			return
		}
	}
}

func main() {
	mode := flag.String("mode", "server", "server, bootstrap, or client")
	listen := flag.String("listen", "127.0.0.1:4444", "server UDP address")
	server := flag.String("server", "127.0.0.1:4444", "server UDP address for client mode")
	assets := flag.String("dev-dir", ".sgsp-action-dev", "development TLS/identity asset directory")
	ownerFile := flag.String("owner-file", ".sgsp-action-dev/owner.json", "owner snapshot written by server mode")
	owners := flag.String("owners", "", "comma-separated owner snapshots read by bootstrap mode")
	bootstrap := flag.String("bootstrap", "", "bootstrap address for grouped client mode")
	group := flag.String("group", "", "optional group key for bootstrap client mode")
	flag.Parse()
	material, err := ensureDevelopmentMaterial(*assets)
	if err != nil {
		fatal(err)
	}
	switch *mode {
	case "server", "owner":
		runServer(*listen, *ownerFile, material)
	case "bootstrap":
		runBootstrap(*listen, *owners, material)
	case "client":
		runClient(*server, *bootstrap, *group, material)
	default:
		fatal(fmt.Errorf("unknown mode %q", *mode))
	}
}

type material struct {
	certificate tls.Certificate
	roots       *x509.CertPool
	identityKey ed25519.PrivateKey
}

func ensureDevelopmentMaterial(dir string) (material, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return material{}, err
	}
	paths := struct{ ca, cert, key, identity string }{
		filepath.Join(dir, "ca.pem"), filepath.Join(dir, "server.pem"), filepath.Join(dir, "server-key.pem"), filepath.Join(dir, "identity-key.pem"),
	}
	if _, err := os.Stat(paths.ca); os.IsNotExist(err) {
		if err := generateDevelopmentMaterial(paths.ca, paths.cert, paths.key, paths.identity); err != nil {
			return material{}, err
		}
	}
	caPEM, err := os.ReadFile(paths.ca)
	if err != nil {
		return material{}, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return material{}, fmt.Errorf("invalid development CA")
	}
	certificate, err := tls.LoadX509KeyPair(paths.cert, paths.key)
	if err != nil {
		return material{}, err
	}
	identityPEM, err := os.ReadFile(paths.identity)
	if err != nil {
		return material{}, err
	}
	block, _ := pem.Decode(identityPEM)
	if block == nil || len(block.Bytes) != ed25519.PrivateKeySize {
		return material{}, fmt.Errorf("invalid development identity key")
	}
	return material{certificate: certificate, roots: roots, identityKey: ed25519.PrivateKey(block.Bytes)}, nil
}
func generateDevelopmentMaterial(caPath, certPath, keyPath, identityPath string) error {
	caPublic, caPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	now := time.Now()
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "SGSP Action Development CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caPublic, caPrivate)
	if err != nil {
		return err
	}
	serverPublic, serverPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	serverPrivateDER, err := x509.MarshalPKCS8PrivateKey(serverPrivate)
	if err != nil {
		return err
	}
	serverTemplate := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, serverPublic, caPrivate)
	if err != nil {
		return err
	}
	_, identityPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	for _, file := range []struct {
		path string
		data []byte
		mode os.FileMode
	}{
		{caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0644},
		{certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}), 0644},
		{keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: serverPrivateDER}), 0600},
		{identityPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: identityPrivate}), 0600},
	} {
		if err := os.WriteFile(file.path, file.data, file.mode); err != nil {
			return err
		}
	}
	return nil
}

func runServer(address, ownerFile string, material material) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	packet, err := net.ListenPacket("udp", address)
	if err != nil {
		fatal(err)
	}
	owner := sgsp.Owner{ID: "action-owner", Endpoint: sgsp.Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}
	server, game, err := newActionOwner(material, owner)
	if err != nil {
		_ = packet.Close()
		fatal(err)
	}
	go game.run(ctx)
	ownerJSON, err := json.Marshal(server.Owner())
	if err != nil {
		fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(ownerFile), 0700); err != nil {
		fatal(err)
	}
	if err := os.WriteFile(ownerFile, ownerJSON, 0600); err != nil {
		fatal(err)
	}
	fmt.Printf("action owner listening on %s; snapshot=%s (Ctrl-C to stop)\n", packet.LocalAddr(), ownerFile)
	if err := server.Serve(ctx, packet); err != nil {
		fatal(err)
	}
}

// newActionOwner builds the example's game owner without starting a listener.
// Keeping transport ownership with the caller makes the same production setup
// suitable for integration tests and for hosts with their own lifecycle.
func newActionOwner(material material, owner sgsp.Owner) (sgsp.Server, *actionGame, error) {
	keys := map[string]ed25519.PublicKey{"dev": material.identityKey.Public().(ed25519.PublicKey)}
	verifier, err := identity.NewIdentityVerifier(identity.IdentityConfig{Issuer: devIssuer, Audience: appID, Keys: keys})
	if err != nil {
		return nil, nil, err
	}
	admissions, err := identity.NewAdmissionVerifier(identity.AdmissionConfig{Issuer: devIssuer, Audience: appID, Keys: keys})
	if err != nil {
		return nil, nil, err
	}
	game := newActionGame()
	router := sgsp.NewRouter()
	if err := router.OnEvent(inputType, func(_ context.Context, incoming *sgsp.Incoming) {
		input, err := inputMessage.Codec.Decode(incoming.Payload)
		if err == nil {
			game.enqueue(gameEnvelope{session: incoming.Session, epoch: incoming.Epoch, input: &input})
		}
	}); err != nil {
		return nil, nil, err
	}
	if err := router.OnEvent(commandType, func(_ context.Context, incoming *sgsp.Incoming) {
		command, err := commandMessage.Codec.Decode(incoming.Payload)
		if err == nil {
			game.enqueue(gameEnvelope{session: incoming.Session, epoch: incoming.Epoch, command: &command})
		}
	}); err != nil {
		return nil, nil, err
	}
	if err := router.OnLifecycle(func(_ context.Context, incoming *sgsp.Incoming) {
		if incoming.Lifecycle != nil {
			game.enqueue(gameEnvelope{session: incoming.Session, epoch: incoming.Epoch, lifecycle: incoming.Lifecycle})
		}
	}); err != nil {
		return nil, nil, err
	}
	if err := sgsp.OnCall(router, inventoryRequest, func(ctx context.Context, session sgsp.Session, _ inventoryQuery) (inventoryReply, error) {
		return game.inventory(ctx, session)
	}); err != nil {
		return nil, nil, err
	}
	if err := router.OnStream(streamType, func(_ context.Context, incoming *sgsp.Incoming) {
		payload, err := io.ReadAll(incoming.Stream)
		if err != nil {
			return
		}
		_, _ = incoming.Stream.Write(append([]byte("echo:"), payload...))
		_ = incoming.Stream.CloseWrite()
	}); err != nil {
		return nil, nil, err
	}
	server, err := sgsp.NewServer(sgsp.ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{material.certificate}}, App: sgsp.AppIdentity{ID: appID, Version: "1"}, Owner: owner, Auth: verifier, Admission: admissions, AuthorizeGroup: func(context.Context, sgsp.Principal, string) error { return nil }, CommitGroupClose: func(context.Context, sgsp.AppIdentity, string, sgsp.Owner) error { return nil }, Dispatch: sgsp.DispatchConfig{Mode: sgsp.Handlers, Router: router}})
	if err != nil {
		return nil, nil, err
	}
	return server, game, nil
}
func runBootstrap(address, ownerFiles string, material material) {
	owners := make([]sgsp.Owner, 0)
	for _, path := range strings.Split(ownerFiles, ",") {
		if path == "" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			fatal(err)
		}
		var owner sgsp.Owner
		if err := json.Unmarshal(data, &owner); err != nil {
			fatal(err)
		}
		owners = append(owners, owner)
	}
	packet, err := net.ListenPacket("udp", address)
	if err != nil {
		fatal(err)
	}
	owner := sgsp.Owner{ID: "action-bootstrap", Endpoint: sgsp.Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}
	server, err := newActionBootstrap(material, owner, owners)
	if err != nil {
		_ = packet.Close()
		fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("action bootstrap listening on %s; owners=%s (Ctrl-C to stop)\n", packet.LocalAddr(), ownerFiles)
	if err := server.Serve(ctx, packet); err != nil {
		fatal(err)
	}
}

func newActionBootstrap(material material, owner sgsp.Owner, owners []sgsp.Owner) (sgsp.Server, error) {
	keys := map[string]ed25519.PublicKey{"dev": material.identityKey.Public().(ed25519.PublicKey)}
	verifier, err := identity.NewIdentityVerifier(identity.IdentityConfig{Issuer: devIssuer, Audience: appID, Keys: keys})
	if err != nil {
		return nil, err
	}
	signer, err := identity.NewAdmissionSigner(identity.SignerConfig{Issuer: devIssuer, Audience: appID, KeyID: "dev", PrivateKey: material.identityKey})
	if err != nil {
		return nil, err
	}
	registry := memory.NewRegistry()
	for _, candidate := range owners {
		if err := registry.Register(context.Background(), candidate); err != nil {
			return nil, err
		}
	}
	bootstrap, err := placement.NewBootstrap(placement.BootstrapConfig{App: sgsp.AppIdentity{ID: appID, Version: "1"}, Store: memory.NewStore(), Registry: registry, Signer: signer, AuthorizeGroup: func(context.Context, sgsp.Principal, string) error { return nil }})
	if err != nil {
		return nil, err
	}
	router := sgsp.NewRouter()
	if err := bootstrap.Register(router); err != nil {
		return nil, err
	}
	return sgsp.NewServer(sgsp.ServerConfig{Role: sgsp.BootstrapRole, TLS: &tls.Config{Certificates: []tls.Certificate{material.certificate}}, App: sgsp.AppIdentity{ID: appID, Version: "1"}, Owner: owner, Auth: verifier, Dispatch: sgsp.DispatchConfig{Mode: sgsp.Handlers, Router: router}})
}
func runClient(address, bootstrapAddress, group string, material material) {
	credential := func(context.Context) (sgsp.Credential, error) {
		return sgsp.Credential{Scheme: "jwt", Data: []byte(mintDevelopmentToken(material.identityKey))}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	endpoint := sgsp.Endpoint{Address: address, ServerName: "localhost"}
	updates := make(chan actionUpdate, 1)
	router := sgsp.NewRouter()
	_ = router.OnEvent(updateType, func(_ context.Context, incoming *sgsp.Incoming) {
		update, err := updateMessage.Codec.Decode(incoming.Payload)
		if err != nil {
			return
		}
		select {
		case updates <- update:
		default:
			select {
			case <-updates:
			default:
			}
			select {
			case updates <- update:
			default:
			}
		}
	})
	config := sgsp.ClientConfig{TLS: &tls.Config{RootCAs: material.roots}, App: sgsp.AppIdentity{ID: appID, Version: "1"}, Credentials: credential, Dispatch: sgsp.DispatchConfig{Mode: sgsp.Handlers, Router: router}}
	if bootstrapAddress != "" {
		resolved, err := placement.Resolve(ctx, sgsp.Endpoint{Address: bootstrapAddress, ServerName: "localhost"}, placement.ResolveConfig{App: config.App, TLS: config.TLS, Credentials: credential, GroupKey: group})
		if err != nil {
			fatal(err)
		}
		endpoint, config.GroupKey, config.AdmissionTicket = resolved.Owner.Endpoint, resolved.GroupKey, resolved.AdmissionTicket
		fmt.Printf("resolved owner=%s group=%q\n", endpoint.Address, resolved.GroupKey)
	}
	client, err := sgsp.Dial(ctx, endpoint, config)
	if err != nil {
		fatal(err)
	}
	defer client.Close(context.Background())
	if err := sgsp.Emit(ctx, client.Session(), inputMessage, actionInput{Tick: 1, Move: "right"}, sgsp.SendOptions{Channel: 1, Delivery: sgsp.UnreliableSequenced}); err != nil {
		fatal(err)
	}
	operationID, err := newOperationID()
	if err != nil {
		fatal(err)
	}
	if err := sgsp.Emit(ctx, client.Session(), commandMessage, actionCommand{OperationID: operationID, Action: "collect"}, sgsp.SendOptions{Channel: 3, Delivery: sgsp.ReliableOrdered}); err != nil {
		fatal(err)
	}
	var update actionUpdate
	for update.Coins < 1 {
		select {
		case update = <-updates:
		case <-ctx.Done():
			fatal(ctx.Err())
		}
	}
	inventory, err := sgsp.Call(ctx, client.Session(), inventoryRequest, inventoryQuery{})
	if err != nil {
		fatal(err)
	}
	stream, err := client.Session().OpenStream(ctx, streamType)
	if err != nil {
		fatal(err)
	}
	if _, err := stream.Write([]byte("snapshot bytes")); err != nil {
		fatal(err)
	}
	if err := stream.CloseWrite(); err != nil {
		fatal(err)
	}
	echo, err := io.ReadAll(stream)
	if err != nil {
		fatal(err)
	}
	_ = stream.Close()
	fmt.Printf("update=%+v inventory=%+v stream=%s\n", update, inventory, echo)
}
func newOperationID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}
func mintDevelopmentToken(key ed25519.PrivateKey) string {
	now := time.Now()
	claims := library.RegisteredClaims{Issuer: devIssuer, Subject: "action-player", Audience: library.ClaimStrings{appID}, ExpiresAt: library.NewNumericDate(now.Add(time.Hour)), IssuedAt: library.NewNumericDate(now), NotBefore: library.NewNumericDate(now)}
	token := library.NewWithClaims(library.SigningMethodEdDSA, claims)
	token.Header["typ"], token.Header["kid"] = "sgsp-access+jwt", "dev"
	encoded, err := token.SignedString(key)
	if err != nil {
		panic(err)
	}
	return encoded
}
func fatal(err error) { fmt.Fprintln(os.Stderr, "action:", err); os.Exit(1) }
