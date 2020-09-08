package handlers

import (
	"fmt"
	"database/sql"
	//what is the _ here
	_ "github.com/lib/pq"
	"log"
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
