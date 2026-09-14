package main

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"qattidev/sgsp"
	"qattidev/sgsp/internal/udprelay"
	"qattidev/sgsp/placement"
)

type runningActionServer struct {
	owner sgsp.Owner
	stop  context.CancelFunc
	done  <-chan error
	once  sync.Once
}

func startActionOwner(t *testing.T, material material, id string) *runningActionServer {
	t.Helper()
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, game, err := newActionOwner(material, sgsp.Owner{ID: id, Endpoint: sgsp.Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}})
	if err != nil {
		_ = packet.Close()
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go game.run(ctx)
	go func() { done <- server.Serve(ctx, packet) }()
	running := &runningActionServer{owner: server.Owner(), stop: stop, done: done}
	t.Cleanup(func() { stopActionServer(t, running) })
	return running
}

func startActionBootstrap(t *testing.T, material material, owners []sgsp.Owner) *runningActionServer {
	t.Helper()
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := newActionBootstrap(material, sgsp.Owner{ID: "bootstrap", Endpoint: sgsp.Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, owners)
	if err != nil {
		_ = packet.Close()
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, packet) }()
	running := &runningActionServer{owner: server.Owner(), stop: stop, done: done}
	t.Cleanup(func() { stopActionServer(t, running) })
	return running
}

func stopActionServer(t *testing.T, server *runningActionServer) {
	t.Helper()
	server.once.Do(func() {
		server.stop()
		select {
		case err := <-server.done:
			if err != nil {
				t.Errorf("action server shutdown: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Errorf("action server did not stop")
		}
	})
}

func actionCredential(material material) sgsp.CredentialProvider {
	return func(context.Context) (sgsp.Credential, error) {
		return sgsp.Credential{Scheme: "jwt", Data: []byte(mintDevelopmentToken(material.identityKey))}, nil
	}
}

func dialAction(t *testing.T, ctx context.Context, material material, endpoint sgsp.Endpoint, group, ticket string) (sgsp.Client, <-chan actionUpdate) {
	t.Helper()
	updates := make(chan actionUpdate, 16)
	router := sgsp.NewRouter()
	if err := router.OnEvent(updateType, func(_ context.Context, incoming *sgsp.Incoming) {
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
	}); err != nil {
		t.Fatal(err)
	}
	client, err := sgsp.Dial(ctx, endpoint, sgsp.ClientConfig{
		TLS:             &tls.Config{RootCAs: material.roots},
		App:             sgsp.AppIdentity{ID: appID, Version: "1"},
		Credentials:     actionCredential(material),
		GroupKey:        group,
		AdmissionTicket: ticket,
		Dispatch:        sgsp.DispatchConfig{Mode: sgsp.Handlers, Router: router},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client, updates
}

func exerciseActionProtocol(t *testing.T, ctx context.Context, client sgsp.Client, updates <-chan actionUpdate) {
	t.Helper()
	if err := sgsp.Emit(ctx, client.Session(), inputMessage, actionInput{Tick: 1, Move: "right"}, sgsp.SendOptions{Channel: 1, Delivery: sgsp.UnreliableSequenced}); err != nil {
		t.Fatal(err)
	}
	operationID, err := newOperationID()
	if err != nil {
		t.Fatal(err)
	}
	if err := sgsp.Emit(ctx, client.Session(), commandMessage, actionCommand{OperationID: operationID, Action: "collect"}, sgsp.SendOptions{Channel: 3, Delivery: sgsp.ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case update := <-updates:
			if update.Coins >= 1 {
				if update.SessionEpoch != client.Session().Epoch() {
					t.Fatalf("update epoch = %d, session epoch = %d", update.SessionEpoch, client.Session().Epoch())
				}
				goto inventory
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

inventory:
	inventory, err := sgsp.Call(ctx, client.Session(), inventoryRequest, inventoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if inventory.Coins < 1 || len(inventory.Items) != 2 {
		t.Fatalf("authoritative inventory = %#v", inventory)
	}
	stream, err := client.Session().OpenStream(ctx, streamType)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("snapshot bytes")); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	echo, err := io.ReadAll(stream)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if string(echo) != "echo:snapshot bytes" {
		t.Fatalf("raw stream echo = %q", echo)
	}
}

func TestActionDirectProtocol(t *testing.T) {
	material, err := ensureDevelopmentMaterial(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	owner := startActionOwner(t, material, "direct-owner")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, updates := dialAction(t, ctx, material, owner.owner.Endpoint, "", "")
	exerciseActionProtocol(t, ctx, client, updates)
}

func TestActionReconnectResumesAuthoritativeSession(t *testing.T) {
	material, err := ensureDevelopmentMaterial(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	owner := startActionOwner(t, material, "reconnect-owner")
	upstream, err := net.ResolveUDPAddr("udp", owner.owner.Endpoint.Address)
	if err != nil {
		t.Fatal(err)
	}
	relay, err := udprelay.New("127.0.0.1:0", "127.0.0.1:0", upstream, udprelay.Config{Seed: 42})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, updates := dialAction(t, ctx, material, sgsp.Endpoint{Address: relay.ClientAddr().String(), ServerName: "localhost"}, "", "")
	beforeID, beforeEpoch := client.Session().ID(), client.Session().Epoch()
	relay.SetBlackhole(udprelay.ClientToServer, true)
	relay.SetBlackhole(udprelay.ServerToClient, true)
	waitForActionState(t, ctx, client.Session(), sgsp.Suspended, beforeEpoch)
	relay.SetBlackhole(udprelay.ClientToServer, false)
	relay.SetBlackhole(udprelay.ServerToClient, false)
	waitForActionState(t, ctx, client.Session(), sgsp.Active, beforeEpoch+1)
	if client.Session().ID() != beforeID {
		t.Fatalf("resumed session ID = %x, want %x", client.Session().ID(), beforeID)
	}
	exerciseActionProtocol(t, ctx, client, updates)
}

func waitForActionState(t *testing.T, ctx context.Context, session sgsp.Session, state sgsp.State, atLeastEpoch uint64) {
	t.Helper()
	for session.State() != state || session.Epoch() < atLeastEpoch {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("session state/epoch = %v/%d, want %v/>=%d: %v", session.State(), session.Epoch(), state, atLeastEpoch, ctx.Err())
		}
	}
}

func TestActionBootstrapTwoClientsShareOwner(t *testing.T) {
	material, err := ensureDevelopmentMaterial(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	firstOwner := startActionOwner(t, material, "owner-a")
	secondOwner := startActionOwner(t, material, "owner-b")
	bootstrap := startActionBootstrap(t, material, []sgsp.Owner{firstOwner.owner, secondOwner.owner})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	resolve := func() (placement.Placement, error) {
		return placement.Resolve(ctx, bootstrap.owner.Endpoint, placement.ResolveConfig{
			App:         sgsp.AppIdentity{ID: appID, Version: "1"},
			TLS:         &tls.Config{RootCAs: material.roots},
			Credentials: actionCredential(material),
			GroupKey:    "integration-match",
		})
	}
	placements := make(chan struct {
		placement placement.Placement
		err       error
	}, 2)
	for range 2 {
		go func() {
			value, err := resolve()
			placements <- struct {
				placement placement.Placement
				err       error
			}{value, err}
		}()
	}
	first, second := <-placements, <-placements
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent placement errors = %v / %v", first.err, second.err)
	}
	if first.placement.Owner != second.placement.Owner || first.placement.GroupKey != "integration-match" || second.placement.GroupKey != "integration-match" {
		t.Fatalf("placements diverged: %#v / %#v", first.placement, second.placement)
	}
	firstClient, firstUpdates := dialAction(t, ctx, material, first.placement.Owner.Endpoint, first.placement.GroupKey, first.placement.AdmissionTicket)
	secondClient, secondUpdates := dialAction(t, ctx, material, second.placement.Owner.Endpoint, second.placement.GroupKey, second.placement.AdmissionTicket)
	exerciseActionProtocol(t, ctx, firstClient, firstUpdates)
	exerciseActionProtocol(t, ctx, secondClient, secondUpdates)
}

func TestActionOwnerTerminationDoesNotRelocateGroup(t *testing.T) {
	material, err := ensureDevelopmentMaterial(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	firstOwner := startActionOwner(t, material, "termination-owner-a")
	secondOwner := startActionOwner(t, material, "termination-owner-b")
	bootstrap := startActionBootstrap(t, material, []sgsp.Owner{firstOwner.owner, secondOwner.owner})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	placement, err := placement.Resolve(ctx, bootstrap.owner.Endpoint, placement.ResolveConfig{
		App:         sgsp.AppIdentity{ID: appID, Version: "1"},
		TLS:         &tls.Config{RootCAs: material.roots},
		Credentials: actionCredential(material),
		GroupKey:    "owner-termination-match",
	})
	if err != nil {
		t.Fatal(err)
	}
	client, _ := dialAction(t, ctx, material, placement.Owner.Endpoint, placement.GroupKey, placement.AdmissionTicket)
	selected := firstOwner
	other := secondOwner
	if placement.Owner != firstOwner.owner {
		selected, other = secondOwner, firstOwner
	}
	stopActionServer(t, selected)
	waitForActionState(t, ctx, client.Session(), sgsp.Closed, 1)
	if other.owner == placement.Owner {
		t.Fatal("test setup selected the supposedly live owner")
	}
	// A terminal owner close must stop the client; it must not resolve a fresh
	// placement or silently attach this group to the other registered owner.
	if client.Session().Owner() != placement.Owner {
		t.Fatalf("client owner changed after termination: %#v", client.Session().Owner())
	}
}
