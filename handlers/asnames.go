package handlers
import (
	"log"
	"net/http"
	"fmt"
	"kaleblazer/models"
	"bufio"
	"strings"
	"strconv"
	"github.com/lib/pq"
)
type ASNamesHandler struct {
	l *log.Logger
}
func NewASNamesHandler(l *log.Logger) *ASNamesHandler {
	return &ASNamesHandler{
		l: l,
	}
}
func GetASNames(l *log.Logger) ([]*models.AutonomousSystemNameEntry, error) {
	var AutonomousSystemNames []*models.AutonomousSystemNameEntry
	websiteDirectory := "https://ftp.ripe.net/ripe/asnames/asn.txt"
	resp, err := http.Get(websiteDirectory)
	if err != nil {
		fmt.Println(err)
	}
	defer resp.Body.Close()
	s := bufio.NewScanner(resp.Body)
	for s.Scan() {
		// do something with s.Bytes() or s.Text()
		AutonomousSystemNames = append(AutonomousSystemNames, consumeASNameLine(s.Text()))

	}
	if err := s.Err(); err != nil {
		// handle error
	}
	postgresConnector := connectPostgres(l)
	err = createPostgresTable(postgresConnector)
	if err != nil {
		panic(err)
	}
	err = recordsToPostgres(postgresConnector, AutonomousSystemNames)
	return AutonomousSystemNames, err

}
func consumeASNameLine(line string) *models.AutonomousSystemNameEntry {
	vals := strings.Split(line, " ")
	var asNameEntry models.AutonomousSystemNameEntry
	var asn int 
	var name string
	var cc string
	if len(vals) == 3 {
		asn, _ = strconv.Atoi(vals[0])
		//easy line, no other commas
		name = strings.Trim(vals[1], " ")
		name = strings.Trim(name, ",")
		cc = strings.Trim(vals[2], " ")
	} else if len(vals) > 3 {
		//try a different split, space once to get ASN then r split comma
		asnStr := strings.Split(line, " ")[0]
		cc = line[len(line)-2:]
		name = strings.Trim(line[0:len(line)-3], asnStr)
		asn, _ = strconv.Atoi(asnStr)
		name = strings.Trim(name, " ")
		name = strings.Trim(name, ",")
	} else {
		fmt.Println("Bad Entry")
		fmt.Println(line)
		fmt.Println(vals)
		fmt.Println(len(vals))
		//multiple commas for now just ignore
	}
	asNameEntry.ASN = asn
	asNameEntry.Name = name
	asNameEntry.CountryCode = cc
	return &asNameEntry

}
func connectPostgres(l *log.Logger) *PostgresConnector {
	postgresConnector := NewPostgresConnector(l)
	return postgresConnector
}
func createPostgresTable(postgresConnector *PostgresConnector) error {
	sqlStatement := `
	DROP TABLE IF EXISTS as_names; CREATE TABLE as_names (
		Id SERIAL PRIMARY KEY,
		ASN bigint NOT NULL,
		Name TEXT,
		Country TEXT
	)
	`
	_, err := postgresConnector.db.Exec(sqlStatement)
	if err != nil {
		panic(err)
		return err
	}
	return err
}
func recordsToPostgres(postgresConnector *PostgresConnector, ASNameData []*models.AutonomousSystemNameEntry) error {
	txn, err := postgresConnector.db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := txn.Prepare(pq.CopyIn("as_names", "asn", "name", "country"))
	if err != nil {
		panic(err)
	}
	for _, post := range ASNameData {
		_, err := stmt.Exec(
			post.ASN,
			post.Name,
			post.CountryCode,
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
