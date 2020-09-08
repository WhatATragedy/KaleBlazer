
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
	"sync"
	"os/exec"
	"os"
	"compress/bzip2"
	"io/ioutil"
	"io"
	"net"
	
)
type RibHandler struct {
	l *log.Logger
	archiveURL string
	collectorURL string
	collectors []string
}
func NewRibHandler(l *log.Logger) *RibHandler {
	return &RibHandler{
		l: l,
		archiveURL: "http://archive.routeviews.org/",
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
		panic(err)
		return err
	}
	return err
}
func (ribHandler *RibHandler) connectPostgres(l *log.Logger) *PostgresConnector {
	postgresConnector := NewPostgresConnector(l)
	return postgresConnector
}
func (ribHandler *RibHandler) GetRibs(){
	ribHandler.GetCollectors()
	fmt.Println("Finished Getting Collectors")
	var wg sync.WaitGroup
	for i, collectorName := range ribHandler.collectors {
		wg.Add(1)
		latestCollection := ribHandler.LatestCollection(collectorName)
		fmt.Printf("%v Latest Collection %v\n", collectorName, latestCollection)
		go ribHandler.getFile(&wg, collectorName, latestCollection)
		if i > 5 {
			break
		}
	}
	wg.Wait()
	fmt.Println("Done Collecting Files...")
	fmt.Println("Unzipping Directory...")
	ribHandler.unzipFiles("ribs/")
	fmt.Println("Directory Unzipped")
	fmt.Println("Init BGP Scanner")
	ribHandler.collectBGPScanner("ribs/")
	fmt.Println("Done.")
	postgresConnector := ribHandler.connectPostgres(ribHandler.l)
	ribHandler.createPostgresTable(postgresConnector)
	ribHandler.ConsumeRIBFile(postgresConnector, "/home/ubuntu/go/src/ribSnatcher/parsed_ribs/route-views.amsix-rib.20200904.1600")
}
func (ribHandler *RibHandler) collectBGPScanner(inputDirectory string) {
	fmt.Println("Collecting BPG Scanner Tasks")
	files, err := ioutil.ReadDir(inputDirectory)
	if err != nil {
        fmt.Println(err)
	}
	var wg sync.WaitGroup
    for _, f := range files {
		wg.Add(1)
		go ribHandler.spawnBGPScanner(&wg, fmt.Sprintf("%v%v", inputDirectory, f.Name()), fmt.Sprintf("parsed_ribs/%v", f.Name()))
	}
	wg.Wait()
}
func (ribHandler *RibHandler) spawnBGPScanner(wg *sync.WaitGroup, inputFilepath string, outputFilepath string){
	defer wg.Done()
	fmt.Println("Spawned a BGP Scanner...")
	cmd := exec.Command("/home/ubuntu/bgpscanner/build/bgpscanner", inputFilepath)
	cmd.Stderr = os.Stderr
	outfile, err := os.Create(outputFilepath)
    if err != nil {
        panic(err)
    }
    defer outfile.Close()
    cmd.Stdout = outfile

    err = cmd.Start(); if err != nil {
        panic(err)
    }
    cmd.Wait()
}
func (ribHandler *RibHandler) unzipFile(wg *sync.WaitGroup, outDir string, inputFilepath string) {
	//TODO Remove the old bzip file when done
	defer wg.Done()
	inputFile, err := os.OpenFile(inputFilepath, 0, 0)
	if err != nil {
		panic(err)
	}
	fmt.Println(inputFilepath)
	inputReader := bzip2.NewReader(inputFile)
	out, err := os.Create(strings.Replace(inputFilepath, ".bz2", "", -1))
	if err != nil {
		fmt.Println(err)
	}
	defer out.Close()
	// Write the body to file
	_, err = io.Copy(out, inputReader)
	if err != nil {
		fmt.Println(err)
	}
	os.Remove(inputFilepath)
}
func (ribHandler *RibHandler) unzipFiles(directory string){
	files, err := ioutil.ReadDir(directory)
	if err != nil {
        fmt.Println(err)
	}
	var wg sync.WaitGroup
    for _, f := range files {
		wg.Add(1)
		go ribHandler.unzipFile(&wg, "ribs/", fmt.Sprintf("%v%v", directory, f.Name()))
	}
	wg.Wait()
}
func (ribHandler *RibHandler) getFile(wg *sync.WaitGroup, collectorName string, collectionTime time.Time) {
	// Get the data
	//example request http://archive.routeviews.org/route-views.amsix/bgpdata/2020.09/RIBS/rib.20200901.0000.bz2
	defer wg.Done()
	fmt.Printf("Collecting %v...\n", collectorName)
	collectionMonth := collectionTime.Format("2006.01")
	fileName := fmt.Sprintf("rib.%v.bz2", collectionTime.Format("20060102.1504"))	
	fullURL := 	fmt.Sprintf("%v%v/bgpdata/%v/RIBS/%v",
		ribHandler.archiveURL, 
		collectorName, 
		collectionMonth,
		fileName,
	)
	resp, err := http.Get(fullURL)
	if err != nil {
		fmt.Println(err)
	}
	defer resp.Body.Close()
	// Create the file
	filepath := fmt.Sprintf("ribs/%v-%v", collectorName, fileName)
	out, err := os.Create(filepath)
	if err != nil {
		fmt.Println(err)
	}
	defer out.Close()
	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		fmt.Println(err)
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
	if len(collectorDates) == 0{
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
	if len(collectorDates) == 0{
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
				if strings.Contains(column.Text, "routeviews"){
					if strings.Contains(column.Text, "\n"){
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
	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", ribHandler.collectorURL)
	})
	c.OnResponse(func(r *colly.Response) {
		fmt.Println("Visited", r.Request.URL)
	})
	c.Visit(ribHandler.collectorURL)
	ribHandler.collectors = collectors
	return collectors
}
func (ribHandler *RibHandler) ConsumeRIBFile(postgresConnector *PostgresConnector, filename string) (error) {
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
			ribHandler.BulkInsert(postgresConnector, ribRows)
			ribRows = ribRows[:0]
			fmt.Println("Insert Complete...")
		}
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
	}
	return err
}
func (ribHandler *RibHandler) BulkInsert(postgresConnector *PostgresConnector, ribRows []*models.RIBEntry) error {
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