package main
//TODO
//If there is no valid data when requesting the file then do not create the file
//goroutine for decompression - mrtparse and bzip should be done in one goroutine
//logging needs to be better
//better handling of errors
//too reliant on rib directory
//if the date 0001-01-01 00:00:00 +0000 UTC then couldn't parse the page

import (
	"log"
	"kaleblazerReborn/handlers"
	"os"
)

func main() {
	l := log.New(os.Stdout, "Kale-Blazer ", log.LstdFlags)
	//asNames := handlers.NewASNamesHandler(l)
	// _, err := handlers.GetASNames(l)
	// if err != nil {
	// 	panic(err)
	// }
	// tals := handlers.NewTalHandler(l)
	// tals.ConsumeTals([]string{}, nil)
	rib := handlers.NewRibHandler(l)
	rib.GetRibs()
}
//todo add function to list directory and return a slice of files
