package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	totalChannel := make(chan int64)
	done := make(chan bool)
	var outputChannel = make(chan int)
	var total int64 = 0
	var filesCount, fileProcessed = 0, 0

	start := time.Now()

	err := filepath.Walk("./files",
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			filesCount++
			go getLineItem(path, totalChannel, outputChannel)
			return nil
		})

	if err != nil {
		log.Println(err)
	}

	go func() {
		for t := range totalChannel {
			total += t
		}
	}()

	go func() {
		for outCount := range outputChannel {
			fileProcessed += outCount
			// fmt.Printf("Files Count is: %d, Processed is: %d\n", filesCount, fileProcessed)

			if fileProcessed >= filesCount {
				close(totalChannel)
				close(done)
			}
		}
	}()

	<-done
	elapsed := time.Since(start)
	fmt.Printf("Total Value is: %d Time Taken is: %s\n", total, elapsed.String())
}

func getLineItem(filename string, totalChannel chan int64, outputChannel chan int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineString := scanner.Text()
		lineSlice := strings.Split(lineString, ",")
		totalChannel <- sumTheSlice(lineSlice)
	}
	outputChannel <- 1
}

func sumTheSlice(slice []string) (total int64) {
	for _, r := range slice {
		t, _ := strconv.ParseInt(r, 10, 64)
		total += t
	}
	return
}
