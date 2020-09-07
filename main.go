package main
//TODO
//If there is no valid data when requesting the file then do not create the file
//goroutine for decompression - mrtparse and bzip should be done in one goroutine
//logging needs to be better
//better handling of errors
//too reliant on rib directory
//if the date 0001-01-01 00:00:00 +0000 UTC then couldn't parse the page

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"github.com/gocolly/colly"
	"strings"
	"time"
	"sync"
	"compress/bzip2"
	"io/ioutil"
	"os/exec"
	"log"
	"ribSnatcher/handlers"
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
	postgresConnector := handlers.NewPostgresConnector(l)
	fmt.Println(postgresConnector)
	postgresConnector.CreateRIBTable()
	postgresConnector.ConsumeRIBFile("/home/ubuntu/go/src/ribSnatcher/parsed_ribs/route-views.amsix-rib.20200904.1600")
}
//todo add function to list directory and return a slice of files

func collectBGPScanner(inputDirectory string) {
	fmt.Println("Collecting BPG Scanner Tasks")
	files, err := ioutil.ReadDir(inputDirectory)
	if err != nil {
        fmt.Println(err)
	}
	var wg sync.WaitGroup
    for _, f := range files {
		wg.Add(1)
		go spawnBGPScanner(&wg, fmt.Sprintf("%v%v", inputDirectory, f.Name()), fmt.Sprintf("parsed_ribs/%v", f.Name()))
	}
	wg.Wait()
}
func spawnBGPScanner(wg *sync.WaitGroup, inputFilepath string, outputFilepath string){
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
func unzipFile(wg *sync.WaitGroup, outDir string, inputFilepath string) {
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
func unzipFiles(directory string){
	files, err := ioutil.ReadDir(directory)
	if err != nil {
        fmt.Println(err)
	}
	var wg sync.WaitGroup
    for _, f := range files {
		wg.Add(1)
		go unzipFile(&wg, "ribs/", fmt.Sprintf("%v%v", directory, f.Name()))
	}
	wg.Wait()
}
func mrtParseFile(){

}
func getFile(wg *sync.WaitGroup, collectorName string, url string, collectionTime time.Time) {
	// Get the data
	defer wg.Done()
	fmt.Printf("Collecting %v...\n", collectorName)
	collectionMonth := collectionTime.Format("2006.01")
	fileName := fmt.Sprintf("rib.%v.bz2", collectionTime.Format("20060102.1504"))	
	fullURL := 	fmt.Sprintf("%v%v/bgpdata/%v/RIBS/%v",
		url, 
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
func LatestCollection(collectorName string, url string) time.Time {
	//example url http://archive.routeviews.org/route-views.amsix/bgpdata/2020.09/RIBS/rib.20200901.0000.bz2
	layout := "20060102.1504"
	latestCollectionMonth := LatestMonth(collectorName, url)
	collectorDates := []time.Time{}
	url = fmt.Sprintf("%v%v/bgpdata/%v/RIBS/", url, collectorName, fmt.Sprintf(latestCollectionMonth.Format("2006.01")))
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
func LatestMonth(collectorName string, url string) time.Time {
	layout := "2006.01"
	collectorDates := []time.Time{}
	url = fmt.Sprintf("%v%v/bgpdata/", url, collectorName)
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

func GetCollectors(url string) []string {
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
		fmt.Println("Visiting", url)
	})
	c.OnResponse(func(r *colly.Response) {
		fmt.Println("Visited", r.Request.URL)
	})
	c.Visit(url)
	return collectors
}