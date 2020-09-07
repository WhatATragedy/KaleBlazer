package models
import (
	"time"
)
type talEntry struct {
	Prefix string
	AutonomousSystem int
	SourceDatetime *time.Time
}
type tal struct {
	Name string
	Datetime *time.Time
}