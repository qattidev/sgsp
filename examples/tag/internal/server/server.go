// Package server connects SGSP handlers to a single-owner game loop.
package server

import (
	"context"
	"errors"
	"log"

	"net"
	"sync"
	"time"

	"qattidev/sgsp"
	"qattidev/sgsp/examples/tag/internal/dev"
	"qattidev/sgsp/examples/tag/internal/game"
	"qattidev/sgsp/examples/tag/internal/protocol"
)

const maxSessions = 8 // Allow a third client to receive a useful join error.

type player struct {
	session   sgsp.Session
	epoch     uint64
	snapshots chan snapshotWork
}

type snapshotWork struct {
	epoch uint64
	state protocol.State
}

type work struct {
	session   sgsp.Session
	epoch     uint64
	lifecycle *sgsp.Lifecycle
	join      chan joinResult
	ctx       context.Context
}

type joinResult struct {
	reply protocol.JoinReply
	err   error
}

type inputWork struct {
	session sgsp.Session
	epoch   uint64
	input   protocol.Input
}

type Server struct {
	endpoint sgsp.Server
	logger   *log.Logger
	game     *game.Game
	players  [2]*player
	inbox    chan work
	done     chan struct{}
	inputMu  sync.Mutex
	inputs   map[sgsp.SessionID]inputWork
	ctx      context.Context
	senders  sync.WaitGroup
}

func New(material dev.Material, address string, logger *log.Logger) (*Server, error) {
	auth, err := material.Authenticator()
	if err != nil {
		return nil, err
	}
	s := &Server{logger: logger, game: game.New(), inbox: make(chan work, 32), done: make(chan struct{}),
		inputs: make(map[sgsp.SessionID]inputWork)}
	router := sgsp.NewRouter()
	// Use Incoming here to capture the dispatch epoch. Reading Session.Epoch()
	// later could incorrectly relabel old work after a connection resumes.
	if err := router.OnRequest(protocol.Join.ID, func(ctx context.Context, in *sgsp.Incoming) {
		if _, err := protocol.Join.Input.Decode(in.Payload); err != nil {
			_ = in.Fail(ctx, sgsp.InvalidArgument, "invalid join request")
			return
		}
		result := make(chan joinResult, 1)
		select {
		case s.inbox <- work{session: in.Session, epoch: in.Epoch, join: result, ctx: ctx}:
		case <-ctx.Done():
			return
		case <-s.done:
			return
		}
		select {
		case value := <-result:
			if value.err != nil {
				var failure *sgsp.Error
				if errors.As(value.err, &failure) {
					_ = in.Fail(ctx, failure.Code, failure.Message)
				}
				return
			}
			payload, err := protocol.Join.Output.Encode(value.reply)
			if err != nil {
				_ = in.Fail(ctx, sgsp.Internal, "encode join reply")
				return
			}
			_ = in.Reply(ctx, payload)
		case <-ctx.Done():
		case <-s.done:
		}
	}); err != nil {
		return nil, err
	}
	if err := router.OnEvent(protocol.Inputs.ID, func(_ context.Context, in *sgsp.Incoming) {
		if in.Channel != protocol.InputOptions.Channel || in.Delivery != protocol.InputOptions.Delivery {
			return
		}
		value, err := protocol.Inputs.Codec.Decode(in.Payload)
		if err != nil {
			return
		}
		s.inputMu.Lock()
		// One latest input per session; memory is bounded even before joining.
		if _, exists := s.inputs[in.Session.ID()]; exists || len(s.inputs) < maxSessions {
			s.inputs[in.Session.ID()] = inputWork{session: in.Session, epoch: in.Epoch, input: value}
		}
		s.inputMu.Unlock()
	}); err != nil {
		return nil, err
	}
	if err := router.OnLifecycle(func(_ context.Context, in *sgsp.Incoming) {
		if in.Lifecycle == nil {
			return
		}
		value := *in.Lifecycle
		// Lifecycle records must not be dropped, including SessionEnded whose
		// session context is already canceled. Shutdown still unblocks callbacks.
		select {
		case s.inbox <- work{session: in.Session, epoch: in.Epoch, lifecycle: &value}:
		case <-s.done:
		}
	}); err != nil {
		return nil, err
	}
	limits := sgsp.DefaultLimits()
	limits.MaxSessions = maxSessions
	s.endpoint, err = sgsp.NewServer(sgsp.ServerConfig{TLS: material.ServerTLS(), App: protocol.App,
		Owner: sgsp.Owner{ID: "tag", Endpoint: sgsp.Endpoint{Address: address, ServerName: "localhost"}},
		Auth:  auth, Limits: limits, Dispatch: sgsp.DispatchConfig{Mode: sgsp.Handlers, Router: router}})
	return s, err
}

// Serve owns the game loop and stops it on every transport exit path.
func (s *Server) Serve(ctx context.Context, packet net.PacketConn) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.run(ctx)
	err := s.endpoint.Serve(ctx, packet)
	cancel()
	<-s.done
	s.senders.Wait()
	return err
}

func (s *Server) run(ctx context.Context) {
	defer close(s.done)
	s.ctx = ctx
	ticker := time.NewTicker(time.Second / protocol.TickRate)
	defer ticker.Stop()
	measuredAt, measuredTick := time.Now(), uint64(0)
	for {
		select {
		case w := <-s.inbox:
			s.handle(w)
		case <-ticker.C:
			// State() is also checked every tick: terminal state remains visible
			// even if a transport cannot queue its final lifecycle notification.
			s.reconcile()
			s.inputMu.Lock()
			for id, input := range s.inputs {
				s.applyInput(input)
				delete(s.inputs, id)
			}
			s.inputMu.Unlock()
			if elapsed := time.Since(measuredAt).Seconds(); elapsed >= 1 {
				s.game.State.Hz = float64(s.game.State.Tick-measuredTick) / elapsed
				s.log("simulation %.1f Hz; tick %d; snapshots target 120/s", s.game.State.Hz, s.game.State.Tick)
				measuredAt, measuredTick = time.Now(), s.game.State.Tick
			}
			previous := s.game.State.Scores
			s.game.Step()
			if previous != s.game.State.Scores {
				s.log("score: A %d — B %d", s.game.State.Scores[0], s.game.State.Scores[1])
			}
			if s.game.State.Tick%(protocol.TickRate/protocol.UpdateRate) == 0 {
				s.broadcast()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) handle(w work) {
	if w.lifecycle != nil {
		// Consult current state rather than replaying an obsolete loss record
		// over a newer connection. Ordinary input still needs the epoch fence.
		s.reconcile()
		return
	}
	if w.join == nil {
		return
	}
	if w.ctx.Err() != nil {
		return
	}
	if w.session.State() != sgsp.Active || w.session.Epoch() != w.epoch {
		w.join <- joinResult{err: &sgsp.Error{Code: sgsp.SessionSuspended, Message: "connection changed; retry joining"}}
		return
	}
	s.reconcile()
	for side, p := range s.players {
		if p != nil && p.session.ID() == w.session.ID() {
			w.join <- joinResult{reply: protocol.JoinReply{Side: protocol.Side(side), State: s.game.State}}
			return
		}
	}
	for side, p := range s.players {
		if p == nil {
			p := &player{session: w.session, epoch: w.epoch, snapshots: make(chan snapshotWork, 1)}
			s.players[side] = p
			s.senders.Add(1)
			go s.sendSnapshots(s.ctx, p.session, p.snapshots)
			s.game.SetPlayer(protocol.Side(side), true, true)
			s.log("%s player joined", protocol.Side(side))
			w.join <- joinResult{reply: protocol.JoinReply{Side: protocol.Side(side), State: s.game.State}}
			return
		}
	}
	w.join <- joinResult{err: &sgsp.Error{Code: sgsp.ResourceExhausted, Message: "game full: both player slots are occupied (a reconnecting player keeps their slot for up to 30 seconds)"}}
}

func (s *Server) reconcile() {
	for side, p := range s.players {
		if p == nil {
			continue
		}
		state := p.session.State()
		if state == sgsp.Closed {
			s.players[side] = nil
			s.game.SetPlayer(protocol.Side(side), false, false)
			s.log("%s player left", protocol.Side(side))
			continue
		}
		connected := state == sgsp.Active
		if connected != s.game.State.Connected[side] {
			if connected {
				s.log("%s player resumed", protocol.Side(side))
			} else {
				s.log("%s player disconnected; game paused", protocol.Side(side))
			}
		}
		p.epoch = p.session.Epoch()
		s.game.SetPlayer(protocol.Side(side), true, connected)
	}
}

func (s *Server) applyInput(input inputWork) {
	if input.session.State() != sgsp.Active || input.session.Epoch() != input.epoch {
		return
	}
	for side, p := range s.players {
		if p != nil && p.session.ID() == input.session.ID() && p.epoch == input.epoch {
			s.game.Input(protocol.Side(side), input.input)
			return
		}
	}
}

func (s *Server) broadcast() {
	for _, p := range s.players {
		if p == nil || p.session.State() != sgsp.Active {
			continue
		}
		value := snapshotWork{epoch: p.epoch, state: s.game.State}
		select {
		case p.snapshots <- value:
			continue
		default:
		}
		select {
		case <-p.snapshots:
		default:
		}
		select {
		case p.snapshots <- value:
		default:
		}
	}
}

// QUIC's datagram queue can block when full. One sender per joined session,
// with one replaceable pending snapshot, isolates that wait from simulation.
// Closing the transport on shutdown/loss also unblocks an in-flight Emit.
func (s *Server) sendSnapshots(ctx context.Context, session sgsp.Session, updates <-chan snapshotWork) {
	defer s.senders.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-session.Context().Done():
			return
		case value := <-updates:
			if ctx.Err() != nil || session.State() != sgsp.Active || session.Epoch() != value.epoch {
				continue
			}
			if err := sgsp.Emit(ctx, session, protocol.Snapshots, value.state, protocol.SnapshotOptions); err != nil &&
				ctx.Err() == nil && !errors.Is(err, sgsp.ErrBackpressure) && !errors.Is(err, sgsp.ErrSessionSuspended) && !errors.Is(err, sgsp.ErrSessionClosed) {
				s.log("snapshot: %v", err)
			}
		}
	}
}

func (s *Server) log(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}
