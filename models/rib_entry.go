package models
import (
	"time"
)
type RIBEntry struct {
	prefix string
	autonomousSystemPath string
	originatingIP string
	originatingASN int
	sourceRIB string
	sourceDatetime *time.Time
}