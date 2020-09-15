package handlers

import (
	"bufio"
	"compress/bzip2"
	"fmt"
	"io"
	"kaleblazer/models"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly"
	"github.com/lib/pq"
)

type RibHandler struct {
	l            *log.Logger
	archiveURL   string
	collectorURL string
	collectors   []string
}

func NewRibHandler(l *log.Logger) *RibHandler {
	return &RibHandler{
		l:            l,
		archiveURL:   "http://archive.routeviews.org/",
		collectorURL: "http://www.routeviews.org/routeviews/index.php/collectors/",
	}
}
func (ribHandler *RibHandler) createPostgresTable(postgresConnector *PostgresConnector) error {
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
		return err
	}
	return err
}
func (ribHandler *RibHandler) connectPostgres(l *log.Logger) *PostgresConnector {
	postgresConnector := NewPostgresConnector(l)
	return postgresConnector
}
func (ribHandler *RibHandler) GetRibs() {
	ribHandler.l.Println("[Main] Welcome To Kale Blazer Reborn...")
	ribHandler.GetCollectors()
	ribHandler.l.Println("[Main] Finished Getting Collectors")
	postgresConnector := ribHandler.connectPostgres(ribHandler.l)
	err := ribHandler.createPostgresTable(postgresConnector)
	if err != nil {
		panic(err)
	}
	sem := make(chan struct{}, 10)
	//it's blocking when getting the files so not the 
	var wg sync.WaitGroup
	taskNum := 0 
	for i, collectorName := range ribHandler.collectors {
		wg.Add(1)
		latestCollection := ribHandler.LatestCollection(collectorName)
		//ribHandler.l.Printf("%v Latest Collection %v\n", collectorName, latestCollection)
		go ribHandler.getFile(&wg, collectorName, latestCollection, postgresConnector, sem)
		taskNum++
		if i >= 4 {
			break
		}
		
	}
	wg.Wait()
	ribHandler.l.Printf("[Main] Completed %v Tasks..\n", taskNum)
	//close the channel when all files have been collected as filepaths should have been pushed
	ribHandler.l.Println("[Main] Done Collecting Files...")
}
func (ribHandler *RibHandler) spawnBGPScanner(inputFilepath string, outputFilepath string) {
	ribHandler.l.Printf("[spawnBGPScanner] Spawned a BGP Scanner For %v\n", inputFilepath)
	cmd := exec.Command("/home/ubuntu/bgpscanner/build/bgpscanner", inputFilepath)
	cmd.Stderr = os.Stderr
	outfile, err := os.Create(outputFilepath)
	if err != nil {
		ribHandler.l.Println("Error Creating Output Path for BGPScanner...")
		panic(err)
	}
	defer outfile.Close()
	cmd.Stdout = outfile

	err = cmd.Start()
	if err != nil {
		ribHandler.l.Println("BGPScanner encountered and Error When Running...")
		panic(err)
	}
	cmd.Wait()

}
func (ribHandler *RibHandler) unzipFile(inputFilepath string) string {
	ribHandler.l.Printf("[unzipFile] Spawned an Unzip Goroutine For %v\n", inputFilepath)
	inputFile, err := os.OpenFile(inputFilepath, 0, 0)
	if err != nil {
		ribHandler.l.Println("[unzipFile] Unzip Encountered Error Opening Zip File...")
		panic(err)
	}
	//ribHandler.l.Println(inputFilepath)
	inputReader := bzip2.NewReader(inputFile)
	out, err := os.Create(strings.Replace(inputFilepath, ".bz2", "", -1))
	if err != nil {
		ribHandler.l.Println(err)
	}
	defer out.Close()
	// Write the body to file
	_, err = io.Copy(out, inputReader)
	if err != nil {
		ribHandler.l.Println(err)
	}
	os.Remove(inputFilepath)
	return out.Name()
}
func (ribHandler *RibHandler) getFile(wg *sync.WaitGroup, collectorName string, collectionTime time.Time, postgresConnector *PostgresConnector, sem chan struct{}) {
	// Get the data
	//example request http://archive.routeviews.org/route-views.amsix/bgpdata/2020.09/RIBS/rib.20200901.0000.bz2
	defer wg.Done()
	sem <- struct{}{}        // grab
    defer func() { <-sem }() // release
	ribHandler.l.Printf("[getFile] Collecting %v...\n", collectorName)
	collectionMonth := collectionTime.Format("2006.01")
	fileName := fmt.Sprintf("rib.%v.bz2", collectionTime.Format("20060102.1504"))
	fullURL := fmt.Sprintf("%v%v/bgpdata/%v/RIBS/%v",
		ribHandler.archiveURL,
		collectorName,
		collectionMonth,
		fileName,
	)
	resp, err := http.Get(fullURL)
	if err != nil {
		ribHandler.l.Println(err)
	} else if resp.StatusCode != 200 {
		ribHandler.l.Printf("Error Collecting %v, status code %v...", collectorName, resp.StatusCode)
	} else {
		defer resp.Body.Close()
		// Create the file
		filepath := fmt.Sprintf("ribs/%v-%v", collectorName, fileName)
		out, err := os.Create(filepath)
		if err != nil {
			ribHandler.l.Println(err)
		}
		defer out.Close()
		// Write the body to file
		_, err = io.Copy(out, resp.Body)
		if err != nil {
			ribHandler.l.Println(err)
		}
		ribHandler.l.Printf("[getFile] Starting Parsing of %v", filepath)
		unzippedFile := ribHandler.unzipFile(filepath)
		ribHandler.spawnBGPScanner(unzippedFile, fmt.Sprintf("parsed_ribs/%v", strings.Replace(unzippedFile, "ribs/", "", -1)))
		ribHandler.ConsumeRIBFile(postgresConnector, fmt.Sprintf("parsed_ribs/%v", strings.Replace(unzippedFile, "ribs/", "", -1)))
	}
	
}
func (ribHandler *RibHandler) LatestCollection(collectorName string) time.Time {
	//example url http://archive.routeviews.org/route-views.amsix/bgpdata/2020.09/RIBS/rib.20200901.0000.bz2
	layout := "20060102.1504"
	latestCollectionMonth := ribHandler.LatestMonth(collectorName)
	collectorDates := []time.Time{}
	url := fmt.Sprintf("%v%v/bgpdata/%v/RIBS/", ribHandler.archiveURL, collectorName, fmt.Sprintf(latestCollectionMonth.Format("2006.01")))
	c := colly.NewCollector()
	// On every a element which has href attribute call callback
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		if strings.Contains(e.Text, "rib.") {
			element := strings.Join(strings.Split(e.Text, ".")[1:3], ".")
			colTime, err := time.Parse(layout, element)
			if err == nil {
				collectorDates = append(collectorDates, colTime)
			}
		}
	})
	c.Visit(url)
	if len(collectorDates) == 0 {
		return time.Time{}
	} else {
		return collectorDates[len(collectorDates)-1]
	}

}
func (ribHandler *RibHandler) LatestMonth(collectorName string) time.Time {
	layout := "2006.01"
	collectorDates := []time.Time{}
	url := fmt.Sprintf("%v%v/bgpdata/", ribHandler.archiveURL, collectorName)
	c := colly.NewCollector()
	// On every a element which has href attribute call callback
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		colTime, err := time.Parse(layout, strings.Trim(e.Text, "/"))
		if err == nil {
			collectorDates = append(collectorDates, colTime)
		}
	})
	c.Visit(url)
	if len(collectorDates) == 0 {
		return time.Time{}
	} else {
		return collectorDates[len(collectorDates)-1]
	}
}
func (ribHandler *RibHandler) GetCollectors() []string {
	collectors := []string{}
	c := colly.NewCollector()
	// On every a element which has href attribute call callback
	c.OnHTML("table[id=servTab]", func(e *colly.HTMLElement) {
		e.ForEach("tr", func(_ int, row *colly.HTMLElement) {
			//this will return the row in the table, then need to get more granular and get the elements in columns
			row.ForEach("td", func(_ int, column *colly.HTMLElement) {
				val := ""
				if strings.Contains(column.Text, "routeviews") {
					if strings.Contains(column.Text, "\n") {
						//this is probably the oregon one so trim newline, split and get first
						element := strings.Trim(column.Text, "\n")
						val = strings.Split(element, "\n")[0]
					} else {
						val = strings.Trim(column.Text, " ")
					}
					if val != "" {
						val = strings.Replace(val, ".routeviews.org", "", -1)
						collectors = append(collectors, val)
					}

				}
			})

		})
	})
	// Before making a request print "Visiting ..."
	// c.OnRequest(func(r *colly.Request) {
	// 	ribHandler.l.Println("Visiting", ribHandler.collectorURL)
	// })
	// c.OnResponse(func(r *colly.Response) {
	// 	ribHandler.l.Println("Visited", r.Request.URL)
	// })
	c.Visit(ribHandler.collectorURL)
	ribHandler.collectors = collectors
	return collectors
}
func (ribHandler *RibHandler) ConsumeRIBFile(postgresConnector *PostgresConnector, filename string) error {
	//open the file
	//after 10000 lines have been batched send the statement
	//loop until finished
	ribHandler.l.Println("Created a Postgres Consumer for %v", filename)
	var ribRows []*models.RIBEntry = nil
	datetimeLayout := "20060102.1504"
	file, err := os.Open(filename)
	if err != nil {
		ribHandler.l.Println("[ConsumeRIBFile] Encountered Error While Opening File...")
		panic(err)
	}
	defer file.Close()
	fileVals := strings.Split(filename, "/")
	sourceFile := fileVals[len(fileVals)-1]
	//datetime and source rib from the filename
	// ribHandler.l.Println(fileVals)
	// ribHandler.l.Println(sourceFile)
	tempSlice := strings.Split(sourceFile, ".")
	var datetimeSlice []string
	var sourceRIB string
	if len(tempSlice) == 4 {
		//this is the routeviews.amsix.date.time
		sourceRIBSlice := tempSlice[0:2]
		datetimeSlice = tempSlice[2:4]
		sourceRIB = strings.Join(sourceRIBSlice, ".")
	} else if len(tempSlice) == 3{
		sourceRIB = tempSlice[0]
		datetimeSlice = tempSlice[1:3]
	} else {
		ribHandler.l.Println("Error Parsing File Name For Postgres Conumser...")
		//I should return an err here
		return nil
	}
	// ribHandler.l.Println(tempSlice)

	// ribHandler.l.Println(len(tempSlice))
	
	originatingDatetimeString := strings.Join(datetimeSlice, ".")
	ribHandler.l.Println(originatingDatetimeString)
	originatingDatetime, err := time.Parse(datetimeLayout, originatingDatetimeString)
	if err != nil {
		ribHandler.l.Println("[ConsumeRIBFile] Encountered Error Parsing Datetime...")
		panic(err)
	}
	scanner := bufio.NewScanner(file)
	index := 0
	for scanner.Scan() {
		val := strings.Split(scanner.Text(), "|")
		// ribHandler.l.Println(val)
		// ribHandler.l.Println(len(val))
		// for _, item := range val {
		// 	ribHandler.l.Println(item)
		// }
		//TODO handles AS_SET Attributes better, just dropping them at the moment
		prefix := val[1]
		as_path := val[2]
		originatingIP := val[3]
		pathSlice := strings.Split(as_path, " ")
		originatingASN, err := strconv.Atoi(pathSlice[len(pathSlice)-1])
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
			Prefix:               prefix,
			AutonomousSystemPath: as_path,
			OriginatingIP:        originatingIP,
			OriginatingASN:       originatingASN,
			SourceRIB:            sourceRIB,
			SourceDatetime:       &originatingDatetime,
		}
		ribRows = append(ribRows, ribRow)
		index++
		if index%10000 == 0 {
			// ribHandler.l.Println("Making a Big Insert...")
			ribHandler.BulkInsert(postgresConnector, ribRows)
			ribRows = ribRows[:0]
			// ribHandler.l.Println("Insert Complete...")
		}
	}

	if err := scanner.Err(); err != nil {
		ribHandler.l.Fatal(err)
	}
	return err
}
func (ribHandler *RibHandler) BulkInsert(postgresConnector *PostgresConnector, ribRows []*models.RIBEntry) error {
	txn, err := postgresConnector.db.Begin()
	if err != nil {
		ribHandler.l.Fatal(err)
	}
	stmt, err := txn.Prepare(pq.CopyIn("ribs", "prefix", "aspath", "originatingip", "originatingasn", "sourcerib", "originatingdatetime"))
	if err != nil {
		ribHandler.l.Println("[BulkInsert] Encountered Error while preparing Postgres Transaction...")
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
			ribHandler.l.Println("[BulkInsert] Encountered Error while executing transaction...")
			panic(err)
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		ribHandler.l.Fatal(err)
	}
	err = stmt.Close()
	if err != nil {
		ribHandler.l.Fatal(err)
	}
	err = txn.Commit()
	if err != nil {
		ribHandler.l.Fatal(err)
	}
	return err
}
