package mydynamo

import(
	"log"
	"strconv"
)

//Removes an element at the specified index from a list of ObjectEntry structs
func remove(list []ObjectEntry, index int) []ObjectEntry {
	return append(list[:index], list[index+1:]...)
}

//Returns true if the specified list of ints contains the specified item
func contains(list []int, item int) bool {
	for _, v := range list {
		if v == item {
			return true
		}
	}
	return false
}

//Rotates a preference list by one, so that we can give each node a unique preference list
func RotateServerList(list []DynamoNode) []DynamoNode {
	return append(list[1:], list[0])
}

//Creates a new Context with the specified Vector Clock
func NewContext(vClock VectorClock) Context {
	return Context{
		Clock: vClock,
	}
}

//Creates a new PutArgs struct with the specified members.
func NewPutArgs(key string, context Context, value []byte) PutArgs {
	return PutArgs{
		Key:     key,
		Context: context,
		Value:   value,
	}
}

//Creates a new DynamoNode struct with the specified members
func NewDynamoNode(addr string, port string) DynamoNode {
	return DynamoNode{
		Address: addr,
		Port:    port,
	}
}

func PrintObjectMap(nodeID string, objectMap map[string][]ObjectEntry) {
	log.Println("================")
	log.Println("NODE ID: ", nodeID)
	for key, value := range objectMap {
		var temp string
		for i:=0; i<len(value); i++ {
			temp += "{ "
			for vkey, vvalue := range value[i].Context.Clock.CountMap {
				temp += "["+vkey+","+strconv.Itoa(vvalue)+"]"
			}
			temp += " }"
		} 
		
		log.Println(key, temp)
	}
	log.Println("================")
}
	