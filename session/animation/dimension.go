package animation

import (
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// Dimension displays the dimension change screen to the player.
type Dimension struct{}

// Play ...
func (animation *Dimension) Play(conn Conn, serverGameData minecraft.GameData) {
	var dimension int32
	if conn.ClientGameData().Dimension == packet.DimensionNether {
		dimension = packet.DimensionEnd
	} else {
		dimension = packet.DimensionNether
	}
	sendDimension(conn, serverGameData, dimension, false)
}

// Clear ...
func (animation *Dimension) Clear(conn Conn, serverGameData minecraft.GameData) {
	_ = conn.WritePacketToClient(&packet.PlayStatus{Status: packet.PlayStatusPlayerSpawn})
	sendDimension(conn, serverGameData, packet.DimensionOverworld, true)
}

// sendDimension updates the player's dimension and optionally force-spawns them if playStatus is enabled.
func sendDimension(conn Conn, serverGameData minecraft.GameData, dimension int32, playStatus bool) {
	_ = conn.WritePacketToClient(&packet.ChangeDimension{Dimension: dimension, Position: serverGameData.PlayerPosition})
	_ = conn.WritePacketToClient(&packet.StopSound{StopAll: true})
	_ = conn.WritePacketToClient(&packet.PlayerAction{ActionType: protocol.PlayerActionDimensionChangeDone})
	if playStatus {
		_ = conn.WritePacketToClient(&packet.PlayStatus{
			Status: packet.PlayStatusPlayerSpawn,
		})
	}
}
