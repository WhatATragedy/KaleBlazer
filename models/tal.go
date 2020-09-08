package models
import (
	"time"
)
type TALEntry struct {
	Prefix string
	AutonomousSystem int
	ValidFrom *time.Time 
	SourceRIR string
	SourceDate *time.Time
}

type TAL struct {
	Name string
	Datetime *time.Time
}