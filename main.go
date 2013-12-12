package main

import (
	"flag"
	"fmt"
	"log"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dbrower/noids/noid"
)

type idInfo struct {
	id  string
	pos int
}

func maxPos(positions <-chan idInfo) {
	var max idInfo

	for id := range positions {
		log.Printf("%+v\n", id)
		if id.pos > max.pos {
			max = id
		}
	}
	if max.id != "" {
		fmt.Printf("Maximum id is %v (= %v)\n", max.id, max.pos)
	} else {
		fmt.Printf("No ids matching the tempalte were found. Try removing the final 'k' in the template.\n")
	}
}

func idDecoder(wg *sync.WaitGroup, template string, out chan<- idInfo, names <-chan string) {
	defer wg.Done()
	var n noid.Noid
	n, err := noid.NewNoid(template)
	if err != nil {
		log.Println(err)
		return
	}
	for s := range names {
		// extract noid
		id := strings.TrimPrefix(s, "info%3Afedora%2F")
		id, err = url.QueryUnescape(id)
		if err != nil {
			log.Println(err, s)
			continue
		}

		log.Printf("Decoded %s\n", id)

		// decode
		pos := n.Index(id)
		if pos != -1 {
			out <- idInfo{id: id, pos: pos}
		} else {
			// Hack. Because the noid part is not generated using the given prefix, the
			// checksums won't match. instead, strip off checksum and try again.
			// assumes the template passed in is missing the final 'k'
			pos = n.Index(id[:len(id)-1])
			if pos != -1 {
				out <- idInfo{id: id, pos: pos}
			}
		}
	}
}

func gatherFilenames(out chan <- string, objdir string) {
	err := filepath.Walk(objdir, func(p string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			log.Printf("Found %s\n", path.Base(p))
			out <- path.Base(p)
		}
		return nil
	})
	if err != nil {
		log.Println(err)
	}
	close(out)
}

func main() {
	var objectDir string
	var template string
	var verbose bool

	flag.StringVar(&objectDir, "objdir", ".", "path to Fedora objectStore root")
	flag.StringVar(&template, "template", "", "noid template to use (e.g. \".reeddk\")")
	flag.BoolVar(&verbose, "v", false, "Verbose logging")
	flag.Parse()

	if !verbose {
		log.SetOutput(ioutil.Discard)
	}

	const nworkers int = 10

	var (
		reduce chan idInfo    = make(chan idInfo, 10*nworkers)
		names  chan string    = make(chan string, 10*nworkers)
		wg     sync.WaitGroup // only for the workers
	)

	go gatherFilenames(names, objectDir)

	for i := 0; i < nworkers; i++ {
		wg.Add(1)
		go idDecoder(&wg, template, reduce, names)
	}

	go func() {
		wg.Wait()
		close(reduce)
	}()

	maxPos(reduce)
}
