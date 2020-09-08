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
	"kaleblazer/handlers"
	"os"
)

func main() {
	l := log.New(os.Stdout, "Kale-Blazer ", log.LstdFlags)
	// archiveURL := "http://archive.routeviews.org/"
	// collectorUrl := "http://www.routeviews.org/routeviews/index.php/collectors/"
	// collectors := GetCollectors(collectorUrl)
	// fmt.Println("Finished Getting Collectors")
	// var wg sync.WaitGroup
	// for _, collectorName := range collectors {
	// 	wg.Add(1)
	// 	latestCollection := LatestCollection(collectorName, archiveURL)
	// 	fmt.Printf("%v Latest Collection %v\n", collectorName, latestCollection)
	// 	go getFile(&wg, collectorName, archiveURL, latestCollection)
	// }
	// wg.Wait()
	// fmt.Println("Done Collecting Files...")
	// fmt.Println("Unzipping Directory...")
	// unzipFiles("ribs/")
	// fmt.Println("Directory Unzipped")
	// fmt.Println("Init BGP Scanner")
	// collectBGPScanner("ribs/")
	// fmt.Println("Done.")
	// postgresConnector := handlers.NewPostgresConnector(l)
	// fmt.Println(postgresConnector)
	// postgresConnector.CreateRIBTable()
	// postgresConnector.ConsumeRIBFile("/home/ubuntu/go/src/ribSnatcher/parsed_ribs/route-views.amsix-rib.20200904.1600")
	//asNames := handlers.NewASNamesHandler(l)
	// _, err := handlers.GetASNames(l)
	// if err != nil {
	// 	panic(err)
	// }
	tals := handlers.NewTalHandler(l)
	tals.ConsumeTals([]string{}, nil)
}
//todo add function to list directory and return a slice of files
