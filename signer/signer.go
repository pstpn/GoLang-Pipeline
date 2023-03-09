package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const NumberOfStepsToCalculateMultiHash = 6

// wrapDataSignerMd5 - Calculate md5(data) hash value using sync.Mutex
func wrapDataSignerMd5(mu *sync.Mutex, data string) string {

	mu.Lock()
	md5TempData := DataSignerMd5(data)
	mu.Unlock()

	return md5TempData
}

// calculateSingleHash - Calculate hash value for current input data
func calculateSingleHash(mu *sync.Mutex, inputData string) string {

	var crc32Data, crc32md5Data string

	wg := sync.WaitGroup{}
	wg.Add(1)

	// Starting goroutine for calculating crc32(md5(data))
	go func(tmpData string) {
		defer wg.Done()
		md5TempData := wrapDataSignerMd5(mu, tmpData)
		crc32md5Data = DataSignerCrc32(md5TempData)
	}(inputData)

	// Calculate crc32(data)
	crc32Data = DataSignerCrc32(inputData)

	wg.Wait()

	return crc32Data + "~" + crc32md5Data
}

// SingleHash - Function for getting crc32(data)+"~"+crc32(md5(data))
func SingleHash(in, out chan interface{}) {

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	for inData := range in {

		wg.Add(1)
		data := fmt.Sprintf("%d", inData.(int))

		// Starting goroutine for calculating single hash value
		go func() {
			defer wg.Done()
			out <- calculateSingleHash(&mu, data)
		}()
	}

	wg.Wait()
}

// calculateMultiHash - Calculate multi hash value for current input data
func calculateMultiHash(inputData string) string {

	wg := sync.WaitGroup{}

	outHash := make([]string, NumberOfStepsToCalculateMultiHash)

	for ind := 0; ind < NumberOfStepsToCalculateMultiHash; ind++ {

		curDigit := strconv.Itoa(ind)
		wg.Add(1)

		// Starting goroutine for calculating crc32(th+data))
		go func(curDigit string, elem *string) {
			defer wg.Done()
			*elem = DataSignerCrc32(curDigit + inputData)
		}(curDigit, &outHash[ind])
	}

	wg.Wait()

	return strings.Join(outHash, "")
}

// MultiHash - Function for getting crc32(th+data)), th=0..5
func MultiHash(in, out chan interface{}) {

	wg := sync.WaitGroup{}

	for inData := range in {

		wg.Add(1)
		data := inData.(string)

		// Starting goroutine for calculating multi hash value
		go func() {
			defer wg.Done()
			out <- calculateMultiHash(data)
		}()
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

	var in, out chan interface{}

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

func runJob(wg *sync.WaitGroup, currentJob job, in, out chan interface{}) {

	defer wg.Done()
	currentJob(in, out)
	defer close(out)
}
