/*
 * MIT License
 *
 * Copyright (c) 2020 Frank Kopp
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package main

import (
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/net/html"

	"github.com/frankkopp/workerpool"
)

type NodeType int

const (
	Unknown NodeType = iota
	Folder  NodeType = iota
	File    NodeType = iota
)

type ProjectFile struct {
	Href          string
	Name          string
	NodeType      NodeType
	CrawlStarted  bool
	CrawlFinished bool
	Subnodes      *[]*ProjectFile
}

const (
	domain = "https://github.com"
)

var fileMap map[string]*ProjectFile
var pool *workerpool.WorkerPool
var fileMapLock = sync.Mutex{}

func main() {

	pool = workerpool.NewWorkerPool(4, 500, true)

	root := &ProjectFile{
		Href:          "/frankkopp/FrankyGo",
		Name:          "root",
		NodeType:      Folder,
		CrawlStarted:  false,
		CrawlFinished: false,
	}

	fileMap = make(map[string]*ProjectFile)
	fileMap[root.Href] = root
	complete := make(chan bool)

	inProgress := int32(0)

	go func() {
		for {
			// check if we have found all files
			foundOne := false

			fileMapLock.Lock()
			for _, f := range fileMap {
				if !f.CrawlStarted {
					f.CrawlStarted = true
					atomic.AddInt32(&inProgress, 1)
					pool.QueueJob(f)
				} else if !f.CrawlFinished {
					foundOne = true
				}
			}
			if !foundOne && atomic.LoadInt32(&inProgress) == 0 {
				pool.Close()
			}
			fileMapLock.Unlock()

			runtime.Gosched()
		}
	}()

	go func() {
		for {
			job, done := pool.GetFinishedWait()
			if done {
				complete <- true
				return
			}
			if len(*job.(*ProjectFile).Subnodes) > 0 {
				for _, j := range *job.(*ProjectFile).Subnodes {
					fileMapLock.Lock()
					_, found := fileMap[j.Href]
					if !found {
						fileMap[j.Href] = j
						atomic.CompareAndSwapInt32(&inProgress, 1, 1)
					}
					fileMapLock.Unlock()
				}
			}
			atomic.AddInt32(&inProgress, -1)
		}
	}()

	<-complete

	fmt.Println("FINISHED")

	// Print the Item
	fileMapLock.Lock()
	for f := range fileMap {
		fmt.Println(f)
	}
	fmt.Println("Items: ", len(fileMap))
	fileMapLock.Unlock()
}

func (item *ProjectFile) Id() string {
	return item.Href
}

func (item *ProjectFile) Run() error {
	url := domain + item.Href
	fmt.Println("Getting ", url)
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	newItems := make([]*ProjectFile, 0, 16)

	tokenizer := html.NewTokenizer(resp.Body)
	b := findFiles(tokenizer, &newItems)
	if b {
		item.NodeType = Folder
		item.Subnodes = &newItems
	} else {
		item.NodeType = File
	}
	item.CrawlFinished = true
	return nil
}

func findFiles(tokenizer *html.Tokenizer, newItems *[]*ProjectFile) bool {
	for {
		tt := tokenizer.Next()
		switch {
		case tt == html.ErrorToken:
			return false
		case tt == html.StartTagToken:
			t := tokenizer.Token()
			if t.Data == ("table") && hasClass(&t, "files") {
				findContent(tokenizer, newItems)
				return true
			}
		}
	}
}

func findContent(tokenizer *html.Tokenizer, newItems *[]*ProjectFile) {
	for {
		tt := tokenizer.Next()
		switch {
		case tt == html.ErrorToken:
			return
		case tt == html.StartTagToken:
			t := tokenizer.Token()
			if t.Data == ("td") && hasClass(&t, "content") {
				readContent(tokenizer, newItems)
			}
		}
	}
}

func readContent(tokenizer *html.Tokenizer, newItems *[]*ProjectFile) {
	for {
		tt := tokenizer.Next()
		switch {
		case tt == html.ErrorToken:
			return
		case tt == html.StartTagToken:
			t := tokenizer.Token()
			if t.Data == ("a") && hasClass(&t, "js-navigation-open") {
				*newItems = append(*newItems, getItemLink(&t))
			}
		case tt == html.EndTagToken:
			t := tokenizer.Token()
			if t.Data == ("td") {
				return
			}
		}
	}
}

func getItemLink(at *html.Token) *ProjectFile {
	item := &ProjectFile{}
	for _, a := range at.Attr {
		switch a.Key {
		case "title":
			item.Name = a.Val
		case "href":
			item.Href = a.Val
		}
	}
	return item
}

func hasClass(t *html.Token, class string) bool {
	for _, a := range t.Attr {
		if a.Key == "class" && strings.Contains(a.Val, class) {
			return true
		}
	}
	return false
}
