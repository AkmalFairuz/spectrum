package animation

import (
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type Conn interface {
	WritePacketToClient(pk packet.Packet) error
	WritePacketsToServer(packets []packet.Packet) error
	WritePacketToServer(pk packet.Packet) error
	ClientGameData() minecraft.GameData
}
