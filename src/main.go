package main

import (
	"time"
	"fmt"
)

func main() {
	go binLogListener()

	time.Sleep(2 * time.Minute)
	fmt.Print("Thx for watching, goodbuy")
}
