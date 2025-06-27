package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	spectrumpacket "github.com/cooldogedev/spectrum/server/packet"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// handleServer continuously reads packets from the server and forwards them to the client.
func handleServer(s *Session) {
	defer func() {
		if r := recover(); r != nil {
			s.CloseWithError(fmt.Errorf("panic while handling server packets: %v", r))
			logError(s, "panic while handling server packets", fmt.Errorf("%v", r))
		}
	}()
loop:
	for {
		select {
		case <-s.ctx.Done():
			s.CloseWithError(context.Cause(s.ctx))
			break loop
		default:
		}

		server := s.Server()
		select {
		case <-server.Context().Done():
			if s.Server() != server {
				continue loop
			}
			if err := s.fallback(); err != nil {
				s.CloseWithError(fmt.Errorf("fallback failed: %w", err))
				logError(s, "failed to fallback to a different server", err)
				break loop
			}
			continue loop
		default:
		}

		batch, err := server.ReadPacket()
		if err != nil {
			server.CloseWithError(fmt.Errorf("failed to read packet from server: %w", err))
			continue loop
		}

		if pks, ok := batch.([][]byte); ok {
			for _, pk := range pks {
				ctx := NewContext()
				s.processor.ProcessServerEncoded(ctx, &pk)
				if ctx.Cancelled() {
					continue
				}

				if _, err := s.client.Write(pk); err != nil {
					s.CloseWithError(fmt.Errorf("failed to write packet to client: %w", err))
					logError(s, "failed to write packet to client", err)
					break loop
				}
			}
			continue loop
		}

		shouldFlush := true

		var packets []packet.Packet
		var isPackets bool

		packets, isPackets = batch.([]packet.Packet)
		if !isPackets {
			if pk, ok := batch.(packet.Packet); ok {
				packets = []packet.Packet{pk}
				shouldFlush = false
			} else {
				s.CloseWithError(fmt.Errorf("failed to read packet from server: %w", err))
				logError(s, "failed to read packet from server", err)
				break loop
			}
		}

		for _, pk := range packets {
			switch pk := pk.(type) {
			case *spectrumpacket.Flush:
				_ = s.client.Flush()
			case *spectrumpacket.Latency:
				s.latency.Store(pk.Latency)
			case *spectrumpacket.Transfer:
				if err := s.Transfer(TransferOptions{Address: pk.Addr, Args: pk.Args}); err != nil {
					logError(s, "failed to transfer", err)
				}
			default:
				ctx := NewContext()
				s.processor.ProcessServer(ctx, &pk)
				if ctx.Cancelled() {
					continue
				}

				if s.opts.SyncProtocol {
					for _, latest := range s.client.Proto().ConvertToLatest(pk, s.client) {
						s.tracker.handlePacket(latest)
					}
				} else {
					s.tracker.handlePacket(pk)
				}

				if err := s.client.WritePacket(pk); err != nil {
					s.CloseWithError(fmt.Errorf("failed to write packet to client: %w", err))
					logError(s, "failed to write packet to client", err)
					break loop
				}
			}
		}

		if shouldFlush {
			s.ClientFlush()
		}
	}
}

// handleClient continuously reads packets from the client and forwards them to the server.
func handleClient(s *Session) {
	defer func() {
		if r := recover(); r != nil {
			s.CloseWithError(fmt.Errorf("panic while handling client packets: %v", r))
			logError(s, "panic while handling client packets", fmt.Errorf("%v", r))
		}
	}()

loop:
	for {
		select {
		case <-s.ctx.Done():
			s.CloseWithError(context.Cause(s.ctx))
			break loop
		default:
		}

		packets, err := s.client.ReadPackets()
		if err != nil {
			s.CloseWithError(fmt.Errorf("failed to read packets from client: %w", err))
			logError(s, "failed to read packets from client", err)
			break loop
		}

		if err := handleClientPackets(s, packets); err != nil {
			s.Server().CloseWithError(fmt.Errorf("failed to write packet to server: %w", err))
		}
	}
}

// handleLatency periodically sends the client's current ping and timestamp to the server for latency reporting.
// The client's latency is derived from half of RakNet's round-trip time (RTT).
// To calculate the total latency, we multiply this value by 2.
func handleLatency(s *Session, interval int64) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(interval))
	defer ticker.Stop()
loop:
	for {
		select {
		case <-s.ctx.Done():
			s.CloseWithError(context.Cause(s.ctx))
			break loop
		case <-ticker.C:
			if err := s.Server().WritePacket(&spectrumpacket.Latency{Latency: s.client.Latency().Milliseconds() * 2, Timestamp: time.Now().UnixMilli(), ClientPacketLoss: float32(s.RakNetClientConn().PacketLossPercentage())}); err != nil {
				logError(s, "failed to write latency packet", err)
			}
		}
	}
}

// handleClientPackets processes and forwards the provided packet from the client to the server.
func handleClientPackets(s *Session, packets []packet.Packet) (err error) {
	if err := s.Server().WritePackets(packets); err != nil {
		return err
	}
	return nil
}

// handleFlusher ...
func handleFlusher(s *Session) {
loop:
	for {
		select {
		case <-s.ctx.Done():
			s.CloseWithError(context.Cause(s.ctx))
			break loop
		case <-s.clientFlusher:
			if err := s.client.Flush(); err != nil {
				s.CloseWithError(fmt.Errorf("failed to flush client: %w", err))
				logError(s, "failed to flush client", err)
				break loop
			}
		}
	}
}

func logError(s *Session, msg string, err error) {
	select {
	case <-s.ctx.Done():
		return
	default:
	}

	if !errors.Is(err, context.Canceled) {
		s.logger.Error(msg, "err", err)
	}
}
