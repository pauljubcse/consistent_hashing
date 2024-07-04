package consistent_hashing

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type ConsistentHash struct{
	hashFunc func(data []byte) uint64
	replicas int
	keys []uint64
	hashMap map[uint64]string
	nodeSet map[string]struct{}
	rwMutex sync.RWMutex
}
func NewConsistentHash(replicas int) *ConsistentHash {
	return &ConsistentHash{
		hashFunc:  defaultHash,
		replicas:  replicas,
		hashMap:   make(map[uint64]string),
		nodeSet:   make(map[string]struct{}), //use map as set, key only storage
	}
}

func defaultHash(data []byte) uint64 {
	hash := fnv.New64a()
	hash.Write(data)
	return hash.Sum64()
}
func (ch *ConsistentHash) AddNode(node string){
	ch.rwMutex.Lock()
	defer ch.rwMutex.Unlock()

	ch.nodeSet[node]=struct{}{}

	for i:=0; i<ch.replicas;i++{
		hash := ch.hashFunc([]byte(node+strconv.Itoa(i)))
		ch.keys = append(ch.keys, hash)
		ch.hashMap[hash]=node
	}
	sort.Slice(ch.keys, func(i, j int) bool {return ch.keys[i]<ch.keys[j]})
}

func (ch *ConsistentHash) RemoveNode (node string) {
	ch.rwMutex.Lock()
	defer ch.rwMutex.Unlock()

	if _, exists := ch.nodeSet[node]; !exists{
		return
	}

	for i:=0;i<ch.replicas;i++{
		hash := ch.hashFunc([]byte(node+strconv.Itoa(i)))
		index  := sort.Search(len(ch.keys), func(i int) bool { return ch.keys[i] >= hash})
		if index < len(ch.keys) && ch.keys[index]==hash{
			ch.keys =  append(ch.keys[:index], ch.keys[index+1:]...)
			delete(ch.hashMap, hash)
		}
	}
	delete(ch.nodeSet, node)
}
func (ch * ConsistentHash) GetNode (key string) (string, bool){
	ch.rwMutex.RLock()
	defer ch.rwMutex.RUnlock()
	if len(ch.keys) == 0 {
		return "", false
	}
	hash := ch.hashFunc([]byte(key))
	index := sort.Search(len(ch.keys), func(i int) bool { return ch.keys[i] >= hash})
	if index == len(ch.keys) {
		index=0
	}
	return ch.hashMap[ch.keys[index]], true
}
