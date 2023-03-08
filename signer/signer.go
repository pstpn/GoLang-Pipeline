package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// SingleHash - Функция получения значения хеша: crc32(data)+"~"+crc32(md5(data))
func SingleHash(in, out chan interface{}) {

	// Создание waitgroup
	wg := sync.WaitGroup{}

	// Инициализация массива каналов для входных данных
	inChannels := make([]chan string, MaxInputDataLen)

	// Инициализация массива каналов для выходных данных
	outChannels := make([]chan string, MaxInputDataLen)

	// Инициализация каждого канала
	for i := 0; i < MaxInputDataLen; i++ {
		inChannels[i] = make(chan string, 1)
		outChannels[i] = make(chan string, 1)
	}

	// Функция, реализующая отправку результата
	// дальше по конвейеру по мере готовности
	sendResult := func(i int) {

		defer wg.Done()

		// Сохранение полученных результатов и последующая отправка
		tmpData := <-outChannels[i] + "~" + <-outChannels[i]
		out <- tmpData
	}

	// Цикл по входным данным (пока канал открыт)
	i := 0

	for inData := range in {

		// Добавление кол-ва горутин, которые необходимо будет подождать в конце
		wg.Add(2)

		// Получение данных
		data := fmt.Sprintf("%d", inData.(int))

		// Функция, реализующая вычисление хэша
		go func(data, outData chan string) {

			defer wg.Done()

			// Создание локальной waitgroup для ожидания внутренней горутины
			wg1 := sync.WaitGroup{}
			wg1.Add(1)

			// Получения входных данных
			inputData := <-data

			// Запуск горутины для вычисления crc32(md5(data))
			go func(tmpData string, outData chan string) {

				defer wg1.Done()

				// Вычисление md5(data)
				md5TempData := DataSignerMd5(tmpData)
				// Вычисление crc32(md5(data))
				crc32Data := DataSignerCrc32(md5TempData)

				// Передача данных дальше по конвейеру
				outData <- crc32Data
			}(inputData, outData)

			// Вычисление crc32(data)
			tmpCrc32Data := DataSignerCrc32(inputData)

			// Передача данных дальше по конвейеру
			outData <- tmpCrc32Data

			// Ожидание завершения горутины
			wg1.Wait()
		}(inChannels[i], outChannels[i])

		// Добавление данных во входной канал для хэша
		inChannels[i] <- data

		// Запуск функции отправки данных
		go sendResult(i)

		// Временный сон, чтобы избежать перегрева функции DataSignerMd5
		time.Sleep(12 * time.Millisecond)

		i++
	}

	// Ожидание завершения всех горутин
	wg.Wait()
}

// MultiHash - Функция, реализующая вычисление значения хэша: crc32(th+data)), где th=0..5
func MultiHash(in, out chan interface{}) {

	// Создание waitgroup
	wg := sync.WaitGroup{}

	// Создание массива th для вычисления хэша
	strNums := [6]string{
		"0",
		"1",
		"2",
		"3",
		"4",
		"5",
	}

	// Инициализация массива каналов для выходных данных
	outChannels := make([]chan string, MaxInputDataLen)

	// Инициализация каждого канала выходных данных
	for i := 0; i < MaxInputDataLen; i++ {
		outChannels[i] = make(chan string)
	}

	// Функция, реализующая отправку результата
	// дальше по конвейеру по мере готовности
	sendResult := func(i int) {

		defer wg.Done()

		// Сохранение полученных результатов и последующая отправка
		tmpData := <-outChannels[i]
		out <- tmpData
	}

	// Цикл по входным данным (пока канал открыт)
	i := 0

	for inData := range in {

		// Добавление кол-ва горутин, которые необходимо будет подождать в конце
		wg.Add(2)

		// Получение входных данных
		data := inData.(string)

		// Запуск горутины вычисления хэша
		go func(outChannel chan string) {

			defer wg.Done()

			// Создание локальной waitgroup
			wg1 := sync.WaitGroup{}

			// Создание массива выходного хэша
			outHash := make([]string, 6)

			// Цикл по массиву цифр th
			for ind, curDigit := range strNums {

				// Увеличение кол-ва ожидаемых горутин
				wg1.Add(1)

				// Запуск горутины для вычисления хэша crc32(th+data))
				go func(curDigit string, elem *string) {

					defer wg1.Done()

					// Сон для перестраховки
					time.Sleep(1 * time.Microsecond)

					// Получение выходного хэша
					*elem = DataSignerCrc32(curDigit + data)
				}(curDigit, &outHash[ind])

				// Сон для правильного порядка вычислений
				time.Sleep(10 * time.Millisecond)
			}

			// Ожидание завершения всех горутин
			wg1.Wait()

			// Отправка полученных данных дальше
			outChannel <- strings.Join(outHash, "")

		}(outChannels[i])

		// Запуск функции отправки данных
		go sendResult(i)

		i++
	}

	// Ожидание завершения всех горутин
	wg.Wait()
}

// CombineResults - Функция, реализующая сортировку и конкатенацию всех полученных хэшей
func CombineResults(in, out chan interface{}) {

	// Создание выходного массива
	var result []string

	// Накопление результатов
	for inData := range in {
		result = append(result, fmt.Sprint(inData))
	}

	// Сортировка результатов
	sort.Strings(result)

	// Отправка результатов дальше по конвейеру
	out <- strings.Join(result, "_")
}

// ExecutePipeline - Функция, реализующая конвейерную обработку функций-воркеров, которые что-то делают
func ExecutePipeline(jobs ...job) {

	// Создание waitgroup
	wg := sync.WaitGroup{}

	// Инициализация массива входных каналов
	channels := make([]chan interface{}, len(jobs)+1)

	// Инициализация каждого входного канала
	for ind := 0; ind < len(jobs)+1; ind++ {
		channels[ind] = make(chan interface{})
	}

	// Запуск всех джобов по порядку в горутинах
	for i := 0; i < len(jobs); i++ {

		// Увеличение кол-ва ожидаемых горутин
		wg.Add(1)

		// Запуск конкретного джоба
		go func(i int) {

			defer wg.Done()

			jobs[i](channels[i], channels[i+1])

			// Закрытие канала после завершения работы
			close(channels[i+1])
		}(i)
	}

	// Ожидание завершения всех горутин
	wg.Wait()
}
