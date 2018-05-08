package main

import (
	"time"
	"fmt"
)

func main() {
	go binlogListener()

	time.Sleep(2 * time.Minute)
	fmt.Print("Thx for watching, goodbuy")
}
