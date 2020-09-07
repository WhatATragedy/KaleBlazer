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
	"github.com/lib/pq"
	"log"
	"strings"
	"ribSnatcher/models"
	"time"
	"strconv"
	"net"
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
func (postgresConnector *PostgresConnector) ConsumeRIBFile(filename string) (error) {
	//open the file
	//after 10000 lines have been batched send the statement
	//loop until finished
	var ribRows []*models.RIBEntry = nil
	datetimeLayout := "20060102.1504"
	file, err := os.Open(filename)
    if err != nil {
        log.Fatal(err)
    }
	defer file.Close()
	fileVals := strings.Split(filename, "/")
	sourceFile := fileVals[len(fileVals) - 1]
	//datetime and source rib from the filename
	tempSlice := strings.Split(sourceFile, ".")
	nameSlice := tempSlice[0:2]
	datetimeSlice := tempSlice[2:4]
	// fmt.Println(nameSlice)
	// fmt.Println(datetimeSlice)
	sourceRIB := strings.Join(nameSlice, ".")
	originatingDatetimeString := strings.Join(datetimeSlice, ".")
	originatingDatetime, err := time.Parse(datetimeLayout, originatingDatetimeString)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(file)
	index := 0
    for scanner.Scan() {
		val := strings.Split(scanner.Text(), "|")
		// fmt.Println(val)
		// fmt.Println(len(val))
		// for _, item := range val {
		// 	fmt.Println(item)
		// }
		//TODO handles AS_SET Attributes better, just dropping them at the moment
		prefix := val[1]
		as_path := val[2]
		originatingIP := val[3]
		pathSlice := strings.Split(as_path, " ")
		originatingASN, err := strconv.Atoi(pathSlice[len(pathSlice) - 1])
		if err != nil {
			continue
			//We've hit an AS SET {5400,123} so just skip the line for now
			//panic(err)
		}
		//check originating IP address is valid as getting some weird issues
		//2020/09/07 09:43:26 pq: invalid input syntax for type inet: "2001:7f8:1::a505:1088:1fe80::f27c:c702:5a0a:6437"
		if net.ParseIP(originatingIP) == nil {
			continue
		}
		// &models.RIBEntry{prefix,as_path,originatingIP,originatingASN,sourceRIB,originatingDatetime,}
		var ribRow = &models.RIBEntry{
			Prefix: prefix,
			AutonomousSystemPath: as_path,
			OriginatingIP: originatingIP,
			OriginatingASN: originatingASN,
			SourceRIB: sourceRIB,
			SourceDatetime: &originatingDatetime,
		}
		ribRows = append(ribRows, ribRow)
		index++
		if index % 10000 == 0{
			fmt.Println("Making a Big Insert...")
			postgresConnector.BulkInsert(ribRows)
			ribRows = ribRows[:0]
			fmt.Println("Insert Complete...")
		}
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
	}
	return err
}

// func (postgresConnector *PostgresConnector) BulkInsert(ribRows []*models.RIBEntry) error {
//     valueStrings := make([]string, 0, len(ribRows))
//     valueArgs := make([]interface{}, 0, len(ribRows) * 6)
//     for _, post := range ribRows {
// 		valueStrings = append(valueStrings, "($1, $2, $3, $4, $5, $6)")
// 		valueArgs = append(valueArgs, post.Prefix)
//         valueArgs = append(valueArgs, post.AutonomousSystemPath)
// 		valueArgs = append(valueArgs, post.OriginatingIP)
// 		valueArgs = append(valueArgs, post.OriginatingASN)
// 		valueArgs = append(valueArgs, post.SourceRIB)
// 		valueArgs = append(valueArgs, post.SourceDatetime)
//     }
//     stmt := fmt.Sprintf("INSERT INTO ribs (Prefix, ASPath, OriginatingIP, OriginatingASN, SourceRIB, OriginatingDatetime) VALUES %s", 
// 						strings.Join(valueStrings, ","))
// 	fmt.Println(stmt)
// 	_, err := postgresConnector.db.Exec(stmt, valueArgs...)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return err
// }
func (postgresConnector *PostgresConnector) BulkInsert(ribRows []*models.RIBEntry) error {
	txn, err := postgresConnector.db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := txn.Prepare(pq.CopyIn("ribs", "prefix", "aspath", "originatingip", "originatingasn", "sourcerib", "originatingdatetime"))
	if err != nil {
		panic(err)
	}
	for _, post := range ribRows {
		_, err := stmt.Exec(
			post.Prefix,
			post.AutonomousSystemPath,
			post.OriginatingIP,
			post.OriginatingASN,
			post.SourceRIB,
			post.SourceDatetime,
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

func (postgresConnector *PostgresConnector) CreateRIBTable() error {
	sqlStatement := `
	DROP TABLE IF EXISTS ribs; CREATE TABLE ribs (
		id SERIAL PRIMARY KEY,
		Prefix INET,
		ASPath TEXT,
		OriginatingIP INET,
		OriginatingASN bigint,
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

