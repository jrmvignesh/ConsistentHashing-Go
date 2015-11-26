package main

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"sort"
)

type HashCircle []uint32

type KeyValue struct {
	Key   int    `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

func (hr HashCircle) Len() int {
	return len(hr)
}

func (hr HashCircle) Less(i, j int) bool {
	return hr[i] < hr[j]
}

func (hr HashCircle) Swap(i, j int) {
	hr[i], hr[j] = hr[j], hr[i]
}

type Node struct {
	Id int
	IP string
}

func NewNode(id int, ip string) *Node {
	return &Node{
		Id: id,
		IP: ip,
	}
}

type ConsistentHash struct {
	Nodes     map[uint32]Node
	IsPresent map[int]bool
	Circle    HashCircle
}

func NewConsistentHash() *ConsistentHash {
	return &ConsistentHash{
		Nodes:     make(map[uint32]Node),
		IsPresent: make(map[int]bool),
		Circle:    HashCircle{},
	}
}

func (hr *ConsistentHash) AddNode(node *Node) bool {

	if _, ok := hr.IsPresent[node.Id]; ok {
		return false
	}
	str := hr.ReturnNodeIP(node)
	hr.Nodes[hr.GetHashValue(str)] = *(node)
	hr.IsPresent[node.Id] = true
	hr.SortHashCircle()
	return true
}

func (hr *ConsistentHash) SortHashCircle() {
	hr.Circle = HashCircle{}
	for k := range hr.Nodes {
		hr.Circle = append(hr.Circle, k)
	}
	sort.Sort(hr.Circle)
}

func (hr *ConsistentHash) ReturnNodeIP(node *Node) string {
	return node.IP
}

func (hr *ConsistentHash) GetHashValue(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (hr *ConsistentHash) Get(key string) Node {
	hash := hr.GetHashValue(key)
	i := hr.SearchForNode(hash)
	return hr.Nodes[hr.Circle[i]]
}

func (hr *ConsistentHash) SearchForNode(hash uint32) int {

	i := sort.Search(len(hr.Circle), func(i int) bool { return hr.Circle[i] >= hash })
	if i < len(hr.Circle) {
		if i == len(hr.Circle)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(hr.Circle) - 1
	}
}

func PutKey(circle *ConsistentHash, str string, input string) {

	ipAddress := circle.Get(str)
	address := "http://" + ipAddress.IP + "/keys/" + str + "/" + input
	fmt.Println(address)
	req, err := http.NewRequest("PUT", address, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer resp.Body.Close()
		fmt.Println("PUT Request successfully completed")
	}
}

func GetKey(key string, circle *ConsistentHash) {
	var out KeyValue
	ipAddress := circle.Get(key)
	address := "http://" + ipAddress.IP + "/keys/" + key
	fmt.Println(address)
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(contents, &out)
		result, _ := json.Marshal(out)
		fmt.Println(string(result))
	}
}

func GetAll(address string) {

	var out []KeyValue
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(contents, &out)
		result, _ := json.Marshal(out)
		fmt.Println(string(result))
	}
}
func main() {
	circle := NewConsistentHash()
	circle.AddNode(NewNode(0, "127.0.0.1:3000"))
	circle.AddNode(NewNode(1, "127.0.0.1:3001"))
	circle.AddNode(NewNode(2, "127.0.0.1:3002"))

	fmt.Println("==================")
	PutKey(circle, "1", "a")
	PutKey(circle, "2", "b")
	PutKey(circle, "3", "c")
	PutKey(circle, "4", "d")
	PutKey(circle, "5", "e")
	PutKey(circle, "6", "f")
	PutKey(circle, "7", "g")
	PutKey(circle, "8", "h")
	PutKey(circle, "9", "i")
	PutKey(circle, "10", "j")

	fmt.Println("==================")

	GetKey("1", circle)
	GetKey("2", circle)
	GetKey("3", circle)
	GetKey("4", circle)
	GetKey("5", circle)
	GetKey("6", circle)
	GetKey("7", circle)
	GetKey("8", circle)
	GetKey("9", circle)
	GetKey("10", circle)

	fmt.Println("==================")
	GetAll("http://127.0.0.1:3000/keys")
	fmt.Println("==================")
	GetAll("http://127.0.0.1:3001/keys")
	fmt.Println("==================")
	GetAll("http://127.0.0.1:3002/keys")

}
