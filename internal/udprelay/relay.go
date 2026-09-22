// Package udprelay provides a bounded, deterministic UDP fault-injection
// proxy. It forwards packets without decoding QUIC or SGSP frames.
package udprelay

import (
	"container/heap"
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"sync"
	"time"
)

var ErrHarnessOverload = errors.New("sgsp UDP relay: harness overload")

type Direction uint8

const (
	ClientToServer Direction = iota + 1
	ServerToClient
)

type Config struct {
	RTT, Jitter   time.Duration
	Loss, Reorder float64
	Seed          uint64
	MaxPackets    int
	MaxBytes      int64
	// OnForward observes a successfully forwarded packet direction. It is a
	// relay-harness hook and must return promptly; it receives no packet data.
	OnForward func(Direction)
}

type Stats struct {
	Forwarded, Dropped, PendingPackets, PeakPendingPackets uint64
	PendingBytes, PeakPendingBytes                         int64
	Overloaded                                             bool
}

// Relay accepts one client path at ClientAddr and forwards it to upstream.
// Its distinct upstream socket ensures the two forwarding directions have
// independent UDP source addresses, like an ordinary NAT/proxy path.
type Relay struct {
	client, upstream net.PacketConn
	upstreamAddr     net.Addr

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once

	mu               sync.Mutex
	clientAddr       net.Addr
	pendingPackets   int
	pendingBytes     int64
	peakPackets      int
	peakBytes        int64
	forwarded        uint64
	dropped          uint64
	overloaded       bool
	err              error
	blackholeForward bool
	blackholeReverse bool
	maxPackets       int
	maxBytes         int64

	in chan relayPacket
}

type relayPacket struct {
	direction Direction
	payload   []byte
	target    net.Addr
	at        time.Time
}

// New binds two UDP sockets and starts forwarding. clientListen and
// upstreamListen may use port zero. Configured maximums default to the
// architecture contract of 65,536 scheduled packets and 64 MiB.
func New(clientListen, upstreamListen string, upstream net.Addr, config Config) (*Relay, error) {
	if upstream == nil || config.RTT < 0 || config.Jitter < 0 || config.Loss < 0 || config.Loss > 1 || config.Reorder < 0 || config.Reorder > 1 {
		return nil, errors.New("sgsp UDP relay: invalid configuration")
	}
	if config.MaxPackets == 0 {
		config.MaxPackets = 65_536
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = 64 << 20
	}
	if config.MaxPackets < 1 || config.MaxBytes < 1 {
		return nil, errors.New("sgsp UDP relay: invalid capacity")
	}
	client, err := net.ListenPacket("udp", clientListen)
	if err != nil {
		return nil, err
	}
	upstreamSocket, err := net.ListenPacket("udp", upstreamListen)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	relay := &Relay{client: client, upstream: upstreamSocket, upstreamAddr: upstream, ctx: ctx, cancel: cancel, done: make(chan struct{}), in: make(chan relayPacket, config.MaxPackets), maxPackets: config.MaxPackets, maxBytes: config.MaxBytes}
	go relay.readClient()
	go relay.readUpstream(upstreamSocket)
	go relay.schedule(config)
	return relay, nil
}

func (r *Relay) ClientAddr() net.Addr { return r.client.LocalAddr() }
func (r *Relay) UpstreamAddr() net.Addr {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.upstream.LocalAddr()
}

// RebindUpstream replaces the relay's server-facing UDP socket. Packets sent
// after it returns use a new source port while the client-facing address stays
// stable, allowing a real transport NAT-rebinding test without packet parsing.
func (r *Relay) RebindUpstream(listen string) error {
	if r == nil || listen == "" {
		return errors.New("sgsp UDP relay: invalid upstream rebind")
	}
	replacement, err := net.ListenPacket("udp", listen)
	if err != nil {
		return err
	}
	r.mu.Lock()
	if r.ctx.Err() != nil {
		r.mu.Unlock()
		_ = replacement.Close()
		return context.Canceled
	}
	previous := r.upstream
	r.upstream = replacement
	r.mu.Unlock()
	go r.readUpstream(replacement)
	_ = previous.Close()
	return nil
}

// SetBlackhole drops a complete direction while retaining the sockets. It is
// useful for deterministic loss/reconnect experiments.
func (r *Relay) SetBlackhole(direction Direction, enabled bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	switch direction {
	case ClientToServer:
		r.blackholeForward = enabled
	case ServerToClient:
		r.blackholeReverse = enabled
	}
	r.mu.Unlock()
}

func (r *Relay) Stats() Stats {
	if r == nil {
		return Stats{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return Stats{Forwarded: r.forwarded, Dropped: r.dropped, PendingPackets: uint64(r.pendingPackets), PendingBytes: r.pendingBytes, PeakPendingPackets: uint64(r.peakPackets), PeakPendingBytes: r.peakBytes, Overloaded: r.overloaded}
}
func (r *Relay) Err() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}
func (r *Relay) Close() error {
	if r == nil {
		return nil
	}
	r.once.Do(func() {
		r.cancel()
		_ = r.client.Close()
		r.mu.Lock()
		upstream := r.upstream
		r.mu.Unlock()
		_ = upstream.Close()
	})
	<-r.done
	return nil
}

func (r *Relay) readClient() {
	buffer := make([]byte, 65_535)
	for {
		count, source, err := r.client.ReadFrom(buffer)
		if err != nil {
			return
		}
		payload := append([]byte(nil), buffer[:count]...)
		r.mu.Lock()
		r.clientAddr = source
		r.mu.Unlock()
		r.enqueue(ClientToServer, payload, r.upstreamAddr)
	}
}

func (r *Relay) readUpstream(socket net.PacketConn) {
	buffer := make([]byte, 65_535)
	for {
		count, _, err := socket.ReadFrom(buffer)
		if err != nil {
			return
		}
		payload := append([]byte(nil), buffer[:count]...)
		r.mu.Lock()
		target := r.clientAddr
		r.mu.Unlock()
		if target != nil {
			r.enqueue(ServerToClient, payload, target)
		}
	}
}

func (r *Relay) enqueue(direction Direction, payload []byte, target net.Addr) {
	r.mu.Lock()
	blackholed := r.blackholeForward
	if direction == ServerToClient {
		blackholed = r.blackholeReverse
	}
	if blackholed {
		r.dropped++
		r.mu.Unlock()
		return
	}
	if r.pendingPackets >= r.maxPackets || int64(len(payload)) > r.maxBytes-r.pendingBytes {
		r.failLocked(ErrHarnessOverload)
		r.mu.Unlock()
		return
	}
	r.pendingPackets++
	r.pendingBytes += int64(len(payload))
	if r.pendingPackets > r.peakPackets {
		r.peakPackets = r.pendingPackets
	}
	if r.pendingBytes > r.peakBytes {
		r.peakBytes = r.pendingBytes
	}
	r.mu.Unlock()
	packet := relayPacket{direction: direction, payload: payload, target: target}
	select {
	case r.in <- packet:
	case <-r.ctx.Done():
		r.releasePending(len(payload))
	}
}

func (r *Relay) schedule(config Config) {
	defer close(r.done)
	queues := packetHeap{}
	heap.Init(&queues)
	forward := rand.New(rand.NewPCG(config.Seed, config.Seed^0x9e3779b97f4a7c15))
	reverse := rand.New(rand.NewPCG(config.Seed^0xc2b2ae3d27d4eb4f, config.Seed^0x165667b19e3779f9))
	var timer *time.Timer
	var wake <-chan time.Time
	for {
		if queues.Len() > 0 {
			delay := time.Until(queues[0].at)
			if delay < 0 {
				delay = 0
			}
			if timer == nil {
				timer = time.NewTimer(delay)
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(delay)
			}
			wake = timer.C
		} else {
			wake = nil
		}
		select {
		case packet := <-r.in:
			rng := forward
			if packet.direction == ServerToClient {
				rng = reverse
			}
			if rng.Float64() < config.Loss {
				r.recordDrop(len(packet.payload))
				continue
			}
			packet.at = time.Now().Add(oneWayDelay(config, rng))
			heap.Push(&queues, packet)
		case <-wake:
			now := time.Now()
			for queues.Len() > 0 && !queues[0].at.After(now) {
				packet := heap.Pop(&queues).(relayPacket)
				socket := r.client
				if packet.direction == ClientToServer {
					r.mu.Lock()
					socket = r.upstream
					r.mu.Unlock()
				}
				if _, err := socket.WriteTo(packet.payload, packet.target); err != nil {
					r.recordDrop(len(packet.payload))
					continue
				}
				r.recordForward(len(packet.payload))
				if config.OnForward != nil {
					config.OnForward(packet.direction)
				}
			}
		case <-r.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			for queues.Len() > 0 {
				packet := heap.Pop(&queues).(relayPacket)
				r.releasePending(len(packet.payload))
			}
			for {
				select {
				case packet := <-r.in:
					r.releasePending(len(packet.payload))
				default:
					return
				}
			}
		}
	}
}

func oneWayDelay(config Config, rng *rand.Rand) time.Duration {
	delay := config.RTT / 2
	if config.Jitter > 0 {
		delta := time.Duration(rng.Int64N(int64(config.Jitter)*2+1)) - config.Jitter
		delay += delta
	}
	if delay < 0 {
		delay = 0
	}
	if rng.Float64() < config.Reorder {
		delay += config.RTT / 2
	}
	return delay
}

func (r *Relay) recordDrop(bytes int) {
	r.mu.Lock()
	r.dropped++
	r.pendingPackets--
	r.pendingBytes -= int64(bytes)
	r.mu.Unlock()
}
func (r *Relay) recordForward(bytes int) {
	r.mu.Lock()
	r.forwarded++
	r.pendingPackets--
	r.pendingBytes -= int64(bytes)
	r.mu.Unlock()
}
func (r *Relay) releasePending(bytes int) {
	r.mu.Lock()
	r.pendingPackets--
	r.pendingBytes -= int64(bytes)
	r.mu.Unlock()
}
func (r *Relay) failLocked(err error) {
	if r.overloaded {
		return
	}
	r.overloaded, r.err = true, err
	go r.Close()
}

type packetHeap []relayPacket

func (h packetHeap) Len() int           { return len(h) }
func (h packetHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h packetHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *packetHeap) Push(value any)    { *h = append(*h, value.(relayPacket)) }
func (h *packetHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = relayPacket{}
	*h = old[:last]
	return value
}
