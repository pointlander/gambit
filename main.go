// Copyright 2019 The Gambit Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"math/rand"
	"time"
)

type Packet struct {
	Value    int
	Callback chan bool
}

func NewPacket(value int) Packet {
	return Packet{
		Value:    value,
		Callback: make(chan bool, 8),
	}
}

type Table struct {
	Input  chan Packet
	Head   int
	Last   int
	Buffer []Packet
}

func (t *Table) Add(packet Packet) Packet {
	deleted := t.Buffer[t.Head]
	t.Buffer[t.Head] = packet
	t.Last, t.Head = t.Head, (t.Head+1)%len(t.Buffer)
	return deleted
}

func (t *Table) Delete(i int) {
	t.Buffer[i] = Packet{}
}

type Join struct {
	Left, Right Table
	Done        chan bool
	Output      chan Packet
}

func (j *Join) Start(inLeft, inRight chan Packet) {
	j.Output = make(chan Packet, 256)
	j.Done = make(chan bool, 1)
	j.Left.Input = inLeft
	j.Left.Buffer = make([]Packet, 8)
	j.Right.Input = inRight
	j.Right.Buffer = make([]Packet, 8)

	go func() {
		for {
			select {
			case <-j.Done:
				return
			case packet := <-inLeft:
				if deleted := j.Left.Add(packet); deleted.Callback != nil {
					deleted.Callback <- false
				}
				for i, right := range j.Right.Buffer {
					if packet.Value == right.Value {
						fmt.Println("right", right.Value)
						j.Left.Delete(j.Left.Last)
						j.Right.Delete(i)
						packet.Callback <- true
						right.Callback <- true
						j.Output <- packet
					}
				}
			case packet := <-inRight:
				if deleted := j.Right.Add(packet); deleted.Callback != nil {
					deleted.Callback <- false
				}
				for i, left := range j.Left.Buffer {
					if packet.Value == left.Value {
						fmt.Println("left", left.Value)
						j.Right.Delete(j.Right.Last)
						j.Left.Delete(i)
						packet.Callback <- true
						left.Callback <- true
						j.Output <- packet
					}
				}
			}
		}
	}()

	return
}

func Client(packet Packet, send chan Packet) {
	rnd := rand.New(rand.NewSource(int64(packet.Value)))
	send <- packet
	for {
		status := <-packet.Callback
		if status {
			return
		}
		time.Sleep(time.Duration(rnd.Intn(100)) * time.Millisecond)
		send <- packet
	}
}

const NumberOfPackets = 2048

func main() {
	left, right, join := make(chan Packet, 256), make(chan Packet, 256), Join{}
	join.Start(left, right)
	for i := 0; i < NumberOfPackets; i++ {
		go Client(NewPacket(i+1), left)
	}
	for i := 0; i < NumberOfPackets; i++ {
		go Client(NewPacket(i+1), right)
	}
	for i := 0; i < NumberOfPackets; i++ {
		<-join.Output
	}
	join.Done <- true
}
