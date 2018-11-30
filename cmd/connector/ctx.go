package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

	go func() {
		println("waiting for done")
		<-ctx.Done()
		println("done!")
		d, _ := ctx.Deadline()
		fmt.Println(fmt.Sprintf("------------ %+v", d))
		fmt.Println(fmt.Sprintf("------------ %+v", time.Until(d)))
	}()

	time.Sleep(10 * time.Second)
	cancel()
}
