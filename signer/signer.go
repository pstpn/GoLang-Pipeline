package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

func SingleHash(in, out chan interface{}) {

	wg := sync.WaitGroup{}

	channels := make([]chan string, MaxInputDataLen)
	outChannels := make([]chan string, MaxInputDataLen)
	for i := 0; i < MaxInputDataLen; i++ {
		channels[i] = make(chan string, 1)
		outChannels[i] = make(chan string, 1)
	}

	sendResult := func(i int) {
		defer wg.Done()
		tmpData := <-outChannels[i] + "~" + <-outChannels[i]
		out <- tmpData
	}

	i := 0

	for inData := range in {

		wg.Add(2)

		data := fmt.Sprintf("%d", inData.(int))

		go func(data, outData chan string) {
			defer wg.Done()
			wg1 := sync.WaitGroup{}
			wg1.Add(1)
			inputData := <-data
			go func(tmpData string, outData chan string) {
				defer wg1.Done()
				md5TempData := DataSignerMd5(tmpData)
				crc32Data := DataSignerCrc32(md5TempData)
				outData <- crc32Data
			}(inputData, outData)
			tmpCrc32Data := DataSignerCrc32(inputData)
			outData <- tmpCrc32Data
			wg1.Wait()
		}(channels[i], outChannels[i])

		channels[i] <- data

		go sendResult(i)

		time.Sleep(12 * time.Millisecond)

		i++
	}

	wg.Wait()
}

func MultiHash(in, out chan interface{}) {

	wg := sync.WaitGroup{}

	strNums := [6]string{
		"0",
		"1",
		"2",
		"3",
		"4",
		"5",
	}

	outChannels := make([]chan string, MaxInputDataLen)
	for i := 0; i < MaxInputDataLen; i++ {
		outChannels[i] = make(chan string)
	}

	sendResult := func(i int) {
		defer wg.Done()
		tmpData := <-outChannels[i]
		out <- tmpData
	}

	i := 0

	for inData := range in {

		wg.Add(2)

		data := inData.(string)

		go func(outChannel chan string) {

			defer wg.Done()

			wg1 := sync.WaitGroup{}

			outHash := make([]string, 6)

			for ind, curDigit := range strNums {

				wg1.Add(1)

				go func(curDigit string, elem *string) {
					defer wg1.Done()
					time.Sleep(1 * time.Microsecond)
					*elem = DataSignerCrc32(curDigit + data)
				}(curDigit, &outHash[ind])

				time.Sleep(10 * time.Millisecond)
			}

			wg1.Wait()
			outChannel <- strings.Join(outHash, "")

		}(outChannels[i])

		go sendResult(i)

		i++
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {

	var result []string

	for inData := range in {

		result = append(result, fmt.Sprint(inData))
	}

	sort.Strings(result)
	out <- strings.Join(result, "_")
}

func ExecutePipeline(jobs ...job) {

	wg := sync.WaitGroup{}

	channels := make([]chan interface{}, len(jobs)+1)
	for ind := 0; ind < len(jobs)+1; ind++ {
		channels[ind] = make(chan interface{})
	}

	for i := 0; i < len(jobs); i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			jobs[i](channels[i], channels[i+1])
			close(channels[i+1])
		}(i)
	}

	wg.Wait()
}
