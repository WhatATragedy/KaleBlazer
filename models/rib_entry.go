package models
import (
	"time"
)
type RIBEntry struct {
	Prefix string
	AutonomousSystemPath string
	OriginatingIP string
	OriginatingASN int
	SourceRIB string
	SourceDatetime *time.Time
}