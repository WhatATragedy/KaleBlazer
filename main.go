package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"github.com/gocolly/colly"
	"strings"


)

func main() {
	collectorUrl := "http://www.routeviews.org/routeviews/index.php/collectors/"
	collectors := GetCollectors(collectorUrl)
	fmt.Println("Finished Getting Collectors")
}

// DownloadFile will download a url to a local file. It's efficient because it will
// write as it downloads and not load the whole file into memory.
func DownloadFile(filepath string, url string) error {
	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	return err
}
func GetCollectors(url string) []string {
	collectors := []string{}
	c := colly.NewCollector()
	// On every a element which has href attribute call callback
	c.OnHTML("table[id=servTab]", func(e *colly.HTMLElement) {
		e.ForEach("tr", func(_ int, row *colly.HTMLElement) {
			//this will return the row in the table, then need to get more granular and get the elements in columns
			row.ForEach("td", func(_ int, column *colly.HTMLElement) {
				if strings.Contains(column.Text, "routeviews"){
					if strings.Contains(column.Text, "\n"){
						//this is probably the oregon one so trim newline, split and get first
						element := strings.Trim(column.Text, "\n")
						val := strings.Split(element, "\n")[0]
						collectors = append(collectors, val)
					} else {
						collectors = append(collectors, column.Text)
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