package raft

import "log"

// Debugging
//const Debug = true
const Debug = false
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


const Debug2 = false
func DPrintf2(format string, a ...interface{}) (n int, err error) {
	if Debug2 {
		log.Printf(format, a...)
	}
	return
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}
