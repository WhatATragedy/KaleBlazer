package handlers
type TalHandler struct {
	l *log.Logger
	talDirectory string
}
func NewTalHandler(l *log.Logger) *TalHandler {
	return &TalHandler{
		l: l,
		talDirectory: "https://ftp.ripe.net/rpki/"
	}
}
func (talHandler *TalHandler) tconsumeTals(regions []string, date *time.Time){
	if regions == nil {
		//All Regions
	}

}