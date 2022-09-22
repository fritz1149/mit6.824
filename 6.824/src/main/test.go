package main

import "fmt"
// import "sync"

// var mux sync.Mutex
func p(s string) {
	fmt.Println(s)
}

func ret_int() int {
	fmt.Println("ret_int")
	return 5
}

func test() int {
	defer fmt.Println("test")
	return ret_int()
}

func main() {
	fmt.Println(string(test()))
}