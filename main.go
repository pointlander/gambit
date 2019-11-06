// Copyright 2019 The Gambit Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"

	"github.com/seiflotfy/cuckoofilter"
)

type Packet struct {
	Value    uint64
	Callback chan State
}

func NewPacket(value uint64) Packet {
	return Packet{
		Value:    value,
		Callback: make(chan State, 8),
	}
}

type State struct {
	Joined []Packet
	Fini   bool
	Send   chan State
}

func (s *State) Serialize() []byte {
	data := make([]byte, 8*len(s.Joined))
	for i, packet := range s.Joined {
		base := i * 8
		binary.BigEndian.PutUint64(data[base:base+8], packet.Value)
	}
	return data
}

type Table struct {
	Input  chan State
	Head   int
	Last   int
	Buffer []State
	Filter *cuckoo.Filter
}

func (t *Table) Add(state State) (State, bool) {
	deleted := t.Delete(t.Head)
	if ok := t.Filter.InsertUnique(state.Serialize()); !ok {
		return deleted, false
	}
	t.Buffer[t.Head] = state
	t.Last, t.Head = t.Head, (t.Head+1)%len(t.Buffer)
	return deleted, true
}

func (t *Table) Delete(i int) State {
	deleted := t.Buffer[i]
	if len(deleted.Joined) == 0 {
		return deleted
	}
	if ok := t.Filter.Delete(deleted.Serialize()); !ok {
		panic(fmt.Errorf("state should exist %v", deleted))
	}
	t.Buffer[i] = State{}
	return deleted
}

type Join struct {
	Name        string
	Terminates  bool
	Left, Right Table
	Done        chan bool
	Output      chan State
}

func (j *Join) Start(inLeft, inRight chan State) {
	j.Output = make(chan State, 256)
	j.Done = make(chan bool, 1)
	j.Left.Input = inLeft
	j.Left.Buffer = make([]State, 8)
	j.Left.Filter = cuckoo.NewFilter(1024)
	j.Right.Input = inRight
	j.Right.Buffer = make([]State, 8)
	j.Right.Filter = cuckoo.NewFilter(1024)

	go func() {
		for {
			select {
			case <-j.Done:
				return
			case packet := <-inLeft:
				deleted, ok := j.Left.Add(packet)
				if len(deleted.Joined) > 0 {
					deleted.Send = inLeft
					for _, p := range deleted.Joined {
						p.Callback <- deleted
					}
				}
				if !ok {
					packet.Send = inLeft
					for _, p := range packet.Joined {
						p.Callback <- packet
					}
					break
				}
				for i, right := range j.Right.Buffer {
					if len(right.Joined) == 0 {
						continue
					}
					if packet.Joined[0].Value == right.Joined[0].Value {
						fmt.Println(j.Name, "right", right.Joined[0].Value)
						j.Left.Delete(j.Left.Last)
						j.Right.Delete(i)
						if j.Terminates {
							packet.Fini = j.Terminates
							right.Fini = j.Terminates
							for _, p := range packet.Joined {
								p.Callback <- packet
							}
							for _, p := range right.Joined {
								p.Callback <- right
							}
						}
						packet.Joined = append(packet.Joined, right.Joined...)
						j.Output <- packet
					}
				}
			case packet := <-inRight:
				deleted, ok := j.Right.Add(packet)
				if len(deleted.Joined) > 0 {
					deleted.Send = inRight
					for _, p := range deleted.Joined {
						p.Callback <- deleted
					}
				}
				if !ok {
					packet.Send = inRight
					for _, p := range packet.Joined {
						p.Callback <- packet
					}
					break
				}
				for i, left := range j.Left.Buffer {
					if len(left.Joined) == 0 {
						continue
					}
					if packet.Joined[0].Value == left.Joined[0].Value {
						fmt.Println(j.Name, "left", left.Joined[0].Value)
						j.Right.Delete(j.Right.Last)
						j.Left.Delete(i)
						if j.Terminates {
							packet.Fini = j.Terminates
							left.Fini = j.Terminates
							for _, p := range packet.Joined {
								p.Callback <- packet
							}
							for _, p := range left.Joined {
								p.Callback <- left
							}
						}
						packet.Joined = append(packet.Joined, left.Joined...)
						j.Output <- packet
					}
				}
			}
		}
	}()

	return
}

func Client(state State, send chan State) {
	rnd := rand.New(rand.NewSource(int64(state.Joined[0].Value)))
	send <- state
	for {
		packet := <-state.Joined[0].Callback
		if packet.Fini {
			return
		}
		time.Sleep(time.Duration(rnd.Intn(100)) * time.Millisecond)
		packet.Send <- packet
	}
}

const NumberOfPackets = 2048

func main() {
	left, right, join := make(chan State, 256), make(chan State, 256), Join{Name: "First", Terminates: false}
	join.Start(left, right)
	right2, join2 := make(chan State, 256), Join{Name: "Second", Terminates: true}
	join2.Start(join.Output, right2)
	for i := 0; i < NumberOfPackets; i++ {
		go Client(State{Joined: []Packet{NewPacket(uint64(i + 1))}}, left)
	}
	for i := 0; i < NumberOfPackets; i++ {
		go Client(State{Joined: []Packet{NewPacket(uint64(i + 1))}}, right)
	}
	for i := 0; i < NumberOfPackets; i++ {
		go Client(State{Joined: []Packet{NewPacket(uint64(i + 1))}}, right2)
	}
	for i := 0; i < NumberOfPackets; i++ {
		<-join2.Output
	}
	join.Done <- true
	join2.Done <- true
}
