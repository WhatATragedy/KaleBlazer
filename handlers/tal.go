package handlers
import (
	"log"
	"time"
	"github.com/gocolly/colly"
	"fmt"
	"strings"
	"net/http"
	"kaleblazer/models"
	"bufio"
	"strconv"
	"github.com/lib/pq"
)
type TalHandler struct {
	l *log.Logger
	talDirectory string
	regions []string
}
func NewTalHandler(l *log.Logger) *TalHandler {
	return &TalHandler{
		l: l,
		talDirectory: "https://ftp.ripe.net/rpki/",
	}
}
func (talHandler *TalHandler) ConsumeTals(regions []string, date *time.Time){
	//example TAL request https://ftp.ripe.net/ripe/rpki/afrinic.tal/2020/09/08/roas.csv
	fmt.Println("Consuming TALs...")
	if len(regions) == 0 {
		//All Regions
		talHandler.regions = talHandler.GetTalRegions()
	}
	if date == nil {
		//don't check the dates as don't care
	}
	//probably worth checking here that the date is valid but just get yesterdays date for now.
	layout := "2006/01/02"
	dt := time.Now()
	dtFormatted := dt.Format(layout)
	fmt.Println("About to Loop")
	for _, region := range talHandler.regions {
		fmt.Println(region)
		fullURL := fmt.Sprintf("%v%v%v/roas.csv", talHandler.talDirectory, region, dtFormatted)
		fmt.Println(fullURL)
		talHandler.consumeTal(fullURL, strings.Trim(region, "/"), &dt)

	}
}
func (talHandler *TalHandler) consumeTal(fullURL string, sourceTAL string, sourceDate *time.Time){
	//var talEntries []*models.TALEntry
	_, err := talHandler.getTal(fullURL, sourceTAL, sourceDate)
	if err!= nil {
		panic(err)
	}
	
}
func (talHandler *TalHandler) getTal(fullURL string, sourceTAL string, sourceDate *time.Time) ([]*models.TALEntry, error) {
	var TALVals []*models.TALEntry
	resp, err := http.Get(fullURL)
	if err != nil {
		fmt.Println(err)
	}
	defer resp.Body.Close()
	s := bufio.NewScanner(resp.Body)
	s.Scan() //skip the first line
	for s.Scan() {
		// do something with s.Bytes() or s.Text()
		//AutonomousSystemNames = append(AutonomousSystemNames, consumeASNameLine(s.Text()))
		talHandler.parseTalLine(s.Text())
		tal := talHandler.parseTalLine(s.Text())
		if tal != nil {
			tal.SourceRIR = sourceTAL
			tal.SourceDate = sourceDate
			TALVals = append(TALVals, tal)
		}

	}
	if err := s.Err(); err != nil {
		// handle error
		panic(err)
	}
	postgresConnector := talHandler.connectPostgres(talHandler.l)
	err = talHandler.createPostgresTable(postgresConnector)
	if err != nil {
		panic(err)
	}
	err = talHandler.recordsToPostgres(postgresConnector, TALVals)
	return TALVals, err
}
func (talHandler *TalHandler) recordsToPostgres(postgresConnector *PostgresConnector, TALData []*models.TALEntry) error {
	txn, err := postgresConnector.db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := txn.Prepare(pq.CopyIn("tals", "prefix", "asn", "validfrom", "sourcerir", "sourcedate"))
	if err != nil {
		panic(err)
	}
	for _, post := range TALData {
		_, err := stmt.Exec(
			post.Prefix,
			post.AutonomousSystem,
			post.ValidFrom,
			post.SourceRIR,
			post.SourceDate,
		)
		if err != nil {
			panic(err)
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}
	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
	return err
}
func (talHandler *TalHandler) parseTalLine(line string) *models.TALEntry {
	layout := "2006-01-02 15:04:05"
	vals := strings.Split(line, ",")
	if len(vals) != 6{
		return nil
	} else {
		asn, err := strconv.Atoi(strings.Trim(vals[1], "AS"))
		if err != nil {
			panic(err)
		}
		prefix := vals[2]
		validFrom := vals[4]
		t, err := time.Parse(layout, validFrom)
		tal := models.TALEntry{
			Prefix: prefix,
			AutonomousSystem: asn,
			ValidFrom: &t,
		}
		return &tal
	}
	
}
func (talHandler *TalHandler) GetTalRegions() []string {
	//TODO Scrape the RPKI page for a list of TAL regions
	talRegions := make([]string, 10)
	c := colly.NewCollector()
	// On every a element which has href attribute call callback
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		if strings.Contains(e.Text, "tal") && e.Text != " " {
			talRegions = append(talRegions, strings.Trim(e.Text, " "))
		}
		
	})
	c.Visit(talHandler.talDirectory)
	fmt.Println(talRegions)
	talHandler.regions = talRegions
	return talRegions
}
func (talHandler *TalHandler) getMostRecentDate(){

}
func (talHandler *TalHandler) createPostgresTable(postgresConnector *PostgresConnector) error {
	sqlStatement := `
	DROP TABLE IF EXISTS tals; CREATE TABLE tals (
		Id SERIAL PRIMARY KEY,
		Prefix INET,
		ASN bigint NOT NULL,
		ValidFrom TIMESTAMP,
		SourceRIR TEXT,
		sourceDate TIMESTAMP
	)
	`
	_, err := postgresConnector.db.Exec(sqlStatement)
	if err != nil {
		panic(err)
		return err
	}
	return err
}
func (talHandler *TalHandler) connectPostgres(l *log.Logger) *PostgresConnector {
	postgresConnector := NewPostgresConnector(l)
	return postgresConnector
}