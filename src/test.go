package main

import (
	"fmt"
	"time"
)

var quit chan int = make(chan int)

func loop() {
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		fmt.Println(i)
	}
}
func tran() {
	fmt.Println("开始打印")
	go loop()
	fmt.Println("结束打印")
}

func main() {
	tran()
	time.Sleep(10 * time.Second)
}
