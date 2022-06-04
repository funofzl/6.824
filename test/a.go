/*
 * @Author: lxk
 * @Date: 2022-04-07 16:46:25
 * @LastEditors: lxk
 * @LastEditTime: 2022-04-16 16:10:42
 */

package main

import "fmt"

type A struct {
	a int
	b int
}

func main() {
	m := map[string]string{}
	m["a"] += "aaa"
	fmt.Println(m)
}
