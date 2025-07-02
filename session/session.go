package session

import (
	"context"
	"errors"
	"fmt"
	"github.com/sandertv/go-raknet"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cooldogedev/spectrum/server"
	"github.com/cooldogedev/spectrum/session/animation"
	"github.com/cooldogedev/spectrum/transport"
	"github.com/cooldogedev/spectrum/util"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// Session represents a player session within the proxy, managing client and server interactions,
// including transfers, fallbacks, and tracking various session states.
type Session struct {
	ctx        context.Context
	cancelFunc context.CancelCauseFunc

	client           *minecraft.Conn
	rakNetClientConn *raknet.Conn

	serverAddr string
	serverConn *server.Conn
	serverMu   sync.RWMutex

	logger   *slog.Logger
	registry *Registry

	discovery server.Discovery
	opts      util.Opts
	transport transport.Transport

	animation animation.Animation
	processor Processor
	tracker   *tracker

	latency      atomic.Int64
	transferring atomic.Bool
	once         sync.Once

	clientDecode map[uint32]struct{}

	clientFlusher chan struct{}

	queuedPacket chan []byte
}

// NewSession creates a new Session instance using the provided minecraft.Conn.
func NewSession(client *minecraft.Conn, logger *slog.Logger, registry *Registry, discovery server.Discovery, opts util.Opts, transport transport.Transport) *Session {
	s := &Session{
		client:           client,
		rakNetClientConn: rakNetClientConn(client),

		logger:   logger,
		registry: registry,

		discovery: discovery,
		opts:      opts,
		transport: transport,

		animation: &animation.Dimension{},
		processor: NopProcessor{},
		tracker:   newTracker(),

		clientDecode: opts.ClientDecodeAsMap(),

		clientFlusher: make(chan struct{}),

		queuedPacket: make(chan []byte, 256),
	}
	s.ctx, s.cancelFunc = context.WithCancelCause(client.Context())
	go handleFlusher(s)
	return s
}

// Login initiates the login sequence with a default timeout of 1 minute.
func (s *Session) Login() (err error) {
	ctx, cancel := context.WithTimeout(s.ctx, time.Minute)
	defer cancel()
	return s.LoginContext(ctx)
}

// LoginTimeout initiates the login sequence with the specified timeout duration.
func (s *Session) LoginTimeout(duration time.Duration) (err error) {
	ctx, cancel := context.WithTimeout(s.ctx, duration)
	defer cancel()
	return s.LoginContext(ctx)
}

// LoginContext initiates the login sequence for the session, including server discovery,
// establishing a connection, and spawning the player in the game. The process is performed
// using the provided context for cancellation.
func (s *Session) LoginContext(ctx context.Context) (err error) {
	identityData := s.client.IdentityData()
	serverAddr, err := s.discovery.Discover(s.client)
	if err != nil {
		s.logger.Debug("discovery failed", "err", err)
		return err
	}

	conn, err := s.dial(ctx, serverAddr, server.ConnectOptions{InitialServer: true})
	if err != nil {
		s.logger.Debug("dialer failed", "err", err)
		return err
	}

	s.serverAddr = serverAddr
	s.serverConn = conn
	if err := conn.ConnectContext(ctx); err != nil {
		s.logger.Debug("connection sequence failed", "err", err)
		return err
	}

	gameData := conn.GameData()
	s.processor.ProcessStartGame(NewContext(), &gameData)
	if err := s.client.StartGame(gameData); err != nil {
		s.logger.Debug("startgame sequence failed", "err", err)
		return err
	}

	conn.SetReady()
	if v, ok := s.Processor().(interface{ ProcessLoginSuccessful(ctx *Context) }); ok {
		v.ProcessLoginSuccessful(NewContext())
	}
	go handleServer(s)
	go handleClient(s)
	go handleLatency(s, s.opts.LatencyInterval)
	s.registry.AddSession(identityData.XUID, s)
	s.logger.Info("logged in session")
	return
}

// Transfer initiates a transfer to a different server using the specified address.
// It sets a default timeout of 1 minute for the transfer operation.
func (s *Session) Transfer(opts TransferOptions) (err error) {
	ctx, cancel := context.WithTimeout(s.ctx, time.Minute)
	defer cancel()
	return s.TransferContext(ctx, opts)
}

// TransferTimeout initiates a transfer to a different server using the specified address
// and a custom timeout duration for the transfer operation.
func (s *Session) TransferTimeout(opts TransferOptions, duration time.Duration) (err error) {
	ctx, cancel := context.WithTimeout(s.ctx, duration)
	defer cancel()
	return s.TransferContext(ctx, opts)
}

// TransferContext initiates a transfer to a different server using the specified address. It ensures that only one transfer
// occurs at a time, returning an error if another transfer is already in progress.
// The process is performed using the provided context for cancellation.
func (s *Session) TransferContext(ctx context.Context, opts TransferOptions) (err error) {
	addr, connectArgs := opts.Address, opts.Args

	if !s.transferring.CompareAndSwap(false, true) {
		return errors.New("already transferring")
	}

	defer s.transferring.Store(false)

	processorCtx := NewContext()
	s.processor.ProcessPreTransfer(processorCtx, &s.serverAddr, &addr)
	if processorCtx.Cancelled() {
		return errors.New("processor failed")
	}

	if s.serverAddr == addr {
		return errors.New("already connected to this server")
	}

	defer func() {
		if err != nil {
			s.sendMetadata(false)
			s.processor.ProcessTransferFailure(NewContext(), &s.serverAddr, &addr)
		}
	}()

	conn, err := s.dial(ctx, addr, server.ConnectOptions{
		InitialServer: false,
		Args:          connectArgs,
	})
	if err != nil {
		s.logger.Debug("dialer failed", "err", err)
		return err
	}

	s.sendMetadata(true)
	if err := conn.ConnectContext(ctx); err != nil {
		conn.CloseWithError(fmt.Errorf("connection sequence failed: %w", err))
		s.logger.Debug("connection sequence failed", "err", err)
		return err
	}

	s.serverMu.Lock()
	if v, ok := s.Processor().(interface {
		ProcessSetServerConn(ctx *Context, srv *server.Conn)
	}); ok {
		v.ProcessSetServerConn(NewContext(), conn)
	}
	_ = s.serverConn.Close()
	s.serverConn = conn
	s.serverMu.Unlock()
	serverGameData := conn.GameData()
	s.animation.Play(s, serverGameData)
	chunk := emptyChunk(serverGameData.Dimension)
	pos := serverGameData.PlayerPosition
	chunkX := int32(pos.X()) >> 4
	chunkZ := int32(pos.Z()) >> 4
	for x := chunkX - 4; x <= chunkX+4; x++ {
		for z := chunkZ - 4; z <= chunkZ+4; z++ {
			_ = s.WritePacketToClient(&packet.LevelChunk{
				Dimension:       serverGameData.Dimension,
				Position:        protocol.ChunkPos{x, z},
				SubChunkCount:   protocol.SubChunkRequestModeLimited,
				HighestSubChunk: 0,
				RawPayload:      chunk,
			})
		}
	}
	s.tracker.clearAll(s)
	_ = s.WritePacketToClient(&packet.MovePlayer{
		EntityRuntimeID: serverGameData.EntityRuntimeID,
		Position:        serverGameData.PlayerPosition,
		Pitch:           serverGameData.Pitch,
		Yaw:             serverGameData.Yaw,
		Mode:            packet.MoveModeReset,
	})
	_ = s.WritePacketToClient(&packet.LevelEvent{EventType: packet.LevelEventStopRaining, EventData: 10_000})
	_ = s.WritePacketToClient(&packet.LevelEvent{EventType: packet.LevelEventStopThunderstorm})
	_ = s.WritePacketToClient(&packet.SetDifficulty{Difficulty: uint32(serverGameData.Difficulty)})
	_ = s.WritePacketToClient(&packet.SetPlayerGameType{GameType: serverGameData.PlayerGameMode})
	_ = s.WritePacketToClient(&packet.GameRulesChanged{GameRules: serverGameData.GameRules})
	_ = s.client.Flush()
	origin := s.serverAddr
	s.animation.Clear(s, serverGameData)
	s.serverAddr = addr
	s.processor.ProcessPostTransfer(NewContext(), &origin, &addr)
	s.logger.Debug("transferred session", "origin", origin, "target", addr)
	return nil
}

// ClientGameData returns the game data of the client.
func (s *Session) ClientGameData() minecraft.GameData {
	return s.client.GameData()
}

// WritePacketToClient ...
func (s *Session) WritePacketToClient(pk packet.Packet) error {
	if v, ok := s.Processor().(interface {
		ProcessWritePacketToClient(ctx *Context, pk *packet.Packet)
	}); ok {
		ctx := NewContext()
		v.ProcessWritePacketToClient(ctx, &pk)
		if ctx.Cancelled() {
			return errors.New("packet write cancelled by processor")
		}
	}
	return s.client.WritePacket(pk)
}

// WritePacketToServer ...
func (s *Session) WritePacketToServer(pk packet.Packet) error {
	return s.WritePacketsToServer([]packet.Packet{pk})
}

// WritePacketsToServer ...
func (s *Session) WritePacketsToServer(packets []packet.Packet) error {
	return s.Server().WritePackets(packets)
}

// Animation returns the animation set to be played during server transfers.
func (s *Session) Animation() animation.Animation {
	return s.animation
}

// SetAnimation sets the animation to be played during server transfers.
func (s *Session) SetAnimation(animation animation.Animation) {
	s.animation = animation
}

// Opts returns the current session options.
func (s *Session) Opts() util.Opts {
	return s.opts
}

// SetOpts updates the session options.
func (s *Session) SetOpts(opts util.Opts) {
	s.opts = opts
	s.clientDecode = opts.ClientDecodeAsMap()
}

// Processor returns the current processor.
func (s *Session) Processor() Processor {
	return s.processor
}

// SetProcessor sets a new processor for the session.
func (s *Session) SetProcessor(processor Processor) {
	s.processor = processor
}

// Latency returns the total latency experienced by the session, combining client and server latencies.
// The client's latency is derived from half of RakNet's round-trip time (RTT).
// To calculate the total latency, we multiply this value by 2.
func (s *Session) Latency() int64 {
	return (s.client.Latency().Milliseconds() * 2) + s.latency.Load()
}

// Client returns the client connection.
func (s *Session) Client() *minecraft.Conn {
	return s.client
}

// RakNetClientConn returns the raknet client connection.
func (s *Session) RakNetClientConn() *raknet.Conn {
	return s.rakNetClientConn
}

// Server returns the current server connection.
func (s *Session) Server() *server.Conn {
	s.serverMu.RLock()
	defer s.serverMu.RUnlock()
	return s.serverConn
}

// Context returns the connection's context. The context is canceled when the session is closed,
// allowing for cancellation of operations that are tied to the lifecycle of the session.
func (s *Session) Context() context.Context {
	return s.ctx
}

// Disconnect sends a packet.Disconnect to the client and closes the session.
func (s *Session) Disconnect(message string) {
	s.CloseWithError(errors.New(message))
}

// ClientFlush ...
func (s *Session) ClientFlush() {
	select {
	case s.clientFlusher <- struct{}{}:
	default:
	}
}

// Close closes the session, including the server and client connections.
func (s *Session) Close() (err error) {
	s.CloseWithError(errors.New("closed by application"))
	return nil
}

func (s *Session) CloseWithError(err error) {
	s.once.Do(func() {
		_ = s.WritePacketToClient(&packet.Disconnect{Message: err.Error()})
		_ = s.client.Close()
		s.processor.ProcessDisconnection(NewContext())
		s.serverMu.RLock()
		if s.serverConn != nil {
			s.serverConn.CloseWithError(err)
		}
		s.serverMu.RUnlock()
		s.registry.RemoveSession(s.client.IdentityData().XUID)
		s.logger.Error("closed session", "error", err)
		close(s.clientFlusher)
	})
}

// dial dials the specified server address and returns a new server.Conn instance.
// The provided context is used to manage timeouts and cancellations during the dialing process.
func (s *Session) dial(ctx context.Context, addr string, options server.ConnectOptions) (*server.Conn, error) {
	select {
	case <-s.ctx.Done():
		return nil, errors.New("session is closed")
	default:
	}

	conn, err := s.transport.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	return server.NewConn(conn, s.client, s.logger.With("addr", addr), s.opts.SyncProtocol, s.opts.Token, options), nil
}

// fallback attempts to transfer the session to a fallback server provided by the discovery.
func (s *Session) fallback() (err error) {
	select {
	case <-s.ctx.Done():
		return context.Cause(s.ctx)
	default:
	}

	addr, err := s.discovery.DiscoverFallback(s.client)
	if err != nil {
		return err
	}

	if err := s.Transfer(TransferOptions{Address: addr}); err != nil {
		return err
	}
	s.logger.Info("transferred session to a fallback server", "addr", addr)
	return
}

// sendMetadata toggles the player's immobility during transfers to prevent position mismatches
// between the client and the server.
func (s *Session) sendMetadata(noAI bool) {
	metadata := protocol.NewEntityMetadata()
	if noAI {
		metadata.SetFlag(protocol.EntityDataKeyFlags, protocol.EntityDataFlagNoAI)
	}
	metadata.SetFlag(protocol.EntityDataKeyFlags, protocol.EntityDataFlagBreathing)
	metadata.SetFlag(protocol.EntityDataKeyFlags, protocol.EntityDataFlagHasGravity)
	_ = s.WritePacketToClient(&packet.SetActorData{
		EntityRuntimeID: s.client.GameData().EntityRuntimeID,
		EntityMetadata:  metadata,
	})
}

func rakNetClientConn(conn *minecraft.Conn) *raknet.Conn {
	rv := reflect.ValueOf(conn).Elem()
	f := rv.FieldByName("conn")
	ret := reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
	return ret.Interface().(*raknet.Conn)
}
