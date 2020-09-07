package models
import (
	"time"
)
type TALEntry struct {
	Prefix string
	AutonomousSystem int
	SourceDatetime *time.Time 
}

type TAL struct {
	Name string
	Datetime *time.Time
}