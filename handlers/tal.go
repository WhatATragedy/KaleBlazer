package handlers
import (
	"log"
	"time"
)
type TalHandler struct {
	l *log.Logger
	talDirectory string
}
func NewTalHandler(l *log.Logger) *TalHandler {
	return &TalHandler{
		l: l,
		talDirectory: "https://ftp.ripe.net/rpki/",
	}
}
func (talHandler *TalHandler) consumeTals(regions []string, date *time.Time){
	if regions == nil {
		//All Regions
	}
	if date == nil {
		//latest date
	}

}
func (talHandler *TalHandler) getTalRegions(){
	//TODO Scrape the RPKI page for a list of TAL regions
}
func (talHandler *TalHandler) getMostRecentDate(){

}