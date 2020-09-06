package handlers

import (
	// "io"
	// "encoding/json"
	"os"
	"bufio"
	"fmt"
	"database/sql"
	//what is the _ here
	_ "github.com/lib/pq"
	"log"
	"strings"
	//"ribSnatcher/models"
)


type PostgresConnector struct {
	l *log.Logger
	db *sql.DB
}
func NewPostgresConnector(l *log.Logger) *PostgresConnector {
	db, err := connectDB()
	if err != nil {
		panic(err)
	}
	return &PostgresConnector{
		l: l,
		db: db,
	}
}

const (
	host     = "localhost"
	port     = 5432
	user     = "user"
	password = "alex"
	dbname   = "routing_information"
)

func connectDB() (*sql.DB, error){
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		  "password=%s dbname=%s sslmode=disable",
		  host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
		return nil, err
	}
	return db, nil
}
func (postgresConnector *PostgresConnector) InsertRIBFile(filename string) (error) {
	//open the file
	//after 10000 lines have been batched send the statement
	//loop until finished
	file, err := os.Open(filename)
    if err != nil {
        log.Fatal(err)
    }
	defer file.Close()
	fileVals := strings.Split(filename, "/")
	sourceFile := fileVals[len(fileVals) - 1]
	//datetime and source rib from the filename
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
		val := strings.Split(scanner.Text(), "|")
		prefix := val[1]
		as_path := val[2]
		originatingIP := val[3]
		pathSlice := strings.Split(val[3])
		originatingASN := pathSlice[len(pathSlice) - 1]
		
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
	}
	return err
}
func (postgresConnector *PostgresConnector) CreateRIBTable() error {
	sqlStatement := `
	DROP TABLE IF EXISTS ribs; CREATE TABLE ribs (
		id SERIAL PRIMARY KEY,
		Prefix INET,
		ASPath TEXT,
		OriginatingIP INET,
		OriginatingASN INT,
		SourceRIB TEXT,
		OriginatingDatetime TIMESTAMP
	)
	`
	_, err := postgresConnector.db.Exec(sqlStatement)
	if err != nil {
		panic(err)
		return err
	}
	return err
}

// for _, row := range unsavedRows {
// 	valueStrings = append(valueStrings, "(?, ?, ?)")
// 	valueArgs = append(valueArgs, post.Column1)
// 	valueArgs = append(valueArgs, post.Column2)
// 	valueArgs = append(valueArgs, post.Column3)
// }
// sqlStatement := `
// DROP TABLE IF EXISTS ribs; CREATE TABLE ribs (
// 	id SERIAL PRIMARY KEY,
// 	Prefix INET,
// 	AS_Path TEXT,
// 	Originating_IP INET,
// 	RIB_Source TEXT,
// 	Originating_Datetime DATETIME,  
// )
// `
// _, err = db.Exec(sqlStatement)
// if err != nil {
// 	panic(err)
// 	return err
// }
// return err

