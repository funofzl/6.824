/*
 * @Author: lxk
 * @Date: 2022-04-07 16:46:25
 * @LastEditors: lxk
 * @LastEditTime: 2022-04-14 19:18:21
 */

package main

import (
	"fmt"
	"log"
)

func pp(format string, val ...interface{}) {
	log.Printf(format, val...)
	return
}

type A struct {
	a int
	b int
}

func main() {
	pp("sdvsdv")
	a := map[int]string{}
	value, _ := a[10]
	fmt.Println(value == "")
	pp("aaaaa")

	delete(a, 999)

	ch := make(chan int, 1)
	select {
	case <-ch:
	default:
	}

	m := make(map[A]int, 1)
	m[A{a: 1, b: 1}] = 99
	fmt.Println(m[A{a: 1, b: 1}])

}
