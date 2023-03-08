package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// numberOfSteps - Number of steps
const numberOfSteps = 6

func InitStringChannels(channels []chan string, count, buf int) {

	for i := 0; i < count; i++ {
		channels[i] = make(chan string, buf)
	}
}

// calculateSingleHash - Calculate hash value for current input data
func calculateSingleHash(mu *sync.Mutex, inData, outData chan string) {

	wg := sync.WaitGroup{}
	wg.Add(1)

	inputData := <-inData

	// Starting goroutine for calculating crc32(md5(data))
	go func(tmpData string, outData chan string) {
		defer wg.Done()
		mu.Lock()
		md5TempData := DataSignerMd5(tmpData)
		mu.Unlock()
		crc32Data := DataSignerCrc32(md5TempData)
		outData <- crc32Data
	}(inputData, outData)

	// Calculate crc32(data)
	tmpCrc32Data := DataSignerCrc32(inputData)
	outData <- tmpCrc32Data

	wg.Wait()
}

// SingleHash - Function for getting crc32(data)+"~"+crc32(md5(data))
func SingleHash(in, out chan interface{}) {

	i := 0

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	inChannels := make([]chan string, MaxInputDataLen)
	outChannels := make([]chan string, MaxInputDataLen)
	InitStringChannels(inChannels, MaxInputDataLen, 1)
	InitStringChannels(outChannels, MaxInputDataLen, 1)

	sendResult := func(i int) {
		defer wg.Done()
		tmpData := <-outChannels[i] + "~" + <-outChannels[i]
		out <- tmpData
	}

	for inData := range in {

		wg.Add(2)
		data := fmt.Sprintf("%d", inData.(int))

		// Starting goroutine for calculating single hash value
		go func(inData, outData chan string) {
			defer wg.Done()
			calculateSingleHash(&mu, inData, outData)
		}(inChannels[i], outChannels[i])

		inChannels[i] <- data
		go sendResult(i)
		i++
	}

	wg.Wait()
}

// calculateMultiHash - Calculate multi hash value for current input data
func calculateMultiHash(inputData string, outChannel chan string) {

	wg := sync.WaitGroup{}

	outHash := make([]string, numberOfSteps)

	for ind := 0; ind < numberOfSteps; ind++ {

		curDigit := strconv.Itoa(ind)
		wg.Add(1)

		// Starting goroutine for calculating crc32(th+data))
		go func(curDigit string, elem *string) {
			defer wg.Done()
			*elem = DataSignerCrc32(curDigit + inputData)
		}(curDigit, &outHash[ind])
	}

	wg.Wait()
	outChannel <- strings.Join(outHash, "")
}

// MultiHash - Function for getting crc32(th+data)), th=0..5
func MultiHash(in, out chan interface{}) {

	i := 0

	wg := sync.WaitGroup{}

	outChannels := make([]chan string, MaxInputDataLen)
	InitStringChannels(outChannels, MaxInputDataLen, 0)

	sendResult := func(i int) {
		defer wg.Done()
		tmpData := <-outChannels[i]
		out <- tmpData
	}

	for inData := range in {

		wg.Add(2)

		// Starting goroutine for calculating multi hash value
		go func(inputData string, outChannel chan string) {
			defer wg.Done()
			calculateMultiHash(inputData, outChannel)
		}(inData.(string), outChannels[i])

		go sendResult(i)
		i++
	}

	wg.Wait()
}

// CombineResults - Function that sorts and concatenates all received hashes
func CombineResults(in, out chan interface{}) {

	var result []string

	for inData := range in {
		result = append(result, fmt.Sprint(inData))
	}

	sort.Strings(result)
	out <- strings.Join(result, "_")
}

// ExecutePipeline - Function that implements pipelining of worker functions that do something
func ExecutePipeline(jobs ...job) {

	wg := sync.WaitGroup{}

	var in chan interface{}
	var out chan interface{}

	// Starting all jobs in goroutines
	for i := 0; i < len(jobs); i++ {
		wg.Add(1)
		out = make(chan interface{})

		// Starting current job
		go runJob(&wg, jobs[i], in, out)

		in = out
	}

	wg.Wait()
}

// runJob - Run current job
func runJob(wg *sync.WaitGroup, currentJob job, in, out chan interface{}) {

	defer wg.Done()
	currentJob(in, out)
	defer close(out)
}
