package mydynamo

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"strconv"
	"errors"
)


type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue         int          //Number of nodes to write to on each Put
	rValue         int          //Number of nodes to read from on each Get
	preferenceList []DynamoNode //Ordered list of other Dynamo nodes to perform operations o
	selfNode       DynamoNode   //This node's address and port info
	nodeID         string       //ID of this node
	crash          bool
	objectMap	   map[string]map[string]ObjectEntry
	noReplicated   []DynamoNode // not replicated node lists

}

func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
	s.preferenceList = incomingList
	return nil
}

// Forces server to gossip
// As this method takes no arguments, we must use the Empty placeholder
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error {
	panic("todo")
}

//Makes server unavailable for some seconds
func (s *DynamoServer) Crash(seconds int, success *bool) error {
	//panic("todo")
	s.crash = true
	go func() { 
		time.Sleep(time.Duration(seconds) * time.Second) 
		s.crash = false
	}() 
	return nil
}

// Put a file to this server and W other servers
func (s *DynamoServer) Put(value PutArgs, result *bool) error {
	if s.crash {
		&result = false
		return errors.New("node crash")
	}
	wValue := s.wValue 
	key := value.Key
	new_context := value.Context
	value := value.Value
	nodeID := s.nodeID
	object := s.objectMap[key][nodeID]
	noReplicated := s.noReplicated
	if object == nil { //meaning this is a new object
		s.objectMap[key][nodeID] = ObjectEntry{NewContext(NewVectorClock()),value}
		&s.objectMap[key][nodeID].Context.Clock.countMap[strconv.Itoa(nodeID)] = 0
	}
	old_context := object.Context
	old_context.Clock.Increment(s.nodeID)
	if old_context.Clock.LessThan(new_context.Clock) { //new context is casually descent from old context, 
		&s.objectMap[key][nodeID].value = value
		&s.objectMap[key][nodeID].Context.Clock.countMap[strconv.Itoa(nodeID)] = new_context.Clock.countMap[strconv.Itoa(nodeID)]
	}else if old_context.Clock.Concurrent(new_context.Clock) {
		// elment-wise max ? 
	}
	var count int
	count = 0
	for i := 0 ; i < len(s.preferenceList); i++ {
		node := s.preferenceList[i]
		serverAddr := node.Address + ":" + node.Port

		e = s.RPCPut(serverAddr, PutFreshContext(key, &s.objectMap[key][nodeID].value)
	}
	


}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
	panic("todo")
}


func (s *DynamoServer) RPCPut(serverAddr, value PutArgs) bool {
	conn, e := rpc.Dial("tcp", serverAddr)
	if e != nil {
		return e
	}

	// perform the call
	success := new(bool)
	e = conn.Call("MyDynamo.PutToPreference", value, success)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	conn.Close()
	return &success
}
func (s *DynamoServer) PutToPreference(value PutArgs, success *bool){
	if s.crash {
		*result = false
		return errors.New("node crash")
	}
	wValue := s.wValue 
	key := value.Key
	new_context := value.Context
	value := value.Value
	nodeID := s.nodeID
	object := s.objectMap[key][nodeID]
	noReplicated := s.noReplicated
	if object == nil { //meaning this is a new object
		s.objectMap[key][nodeID] = ObjectEntry{NewContext(NewVectorClock()),value}
		&s.objectMap[key][nodeID].Context.Clock.countMap[strconv.Itoa(nodeID)] = 0
	}
	old_context := object.Context
	old_context.Clock.Increment(s.nodeID)
	if old_context.Clock.LessThan(new_context.Clock) { //new context is casually descent from old context, 
		&s.objectMap[key][nodeID].Value = make([]byte, len(value))
		copy(&s.objectMap[key][nodeID].Value, value)
		&s.objectMap[key][nodeID].Context.Clock.countMap[strconv.Itoa(nodeID)] = new_context.Clock.countMap[strconv.Itoa(nodeID)]
	}

}



/* Belows are functions that implement server boot up and initialization */
func NewDynamoServer(w int, r int, hostAddr string, hostPort string, id string) DynamoServer {
	preferenceList := make([]DynamoNode, 0)
	selfNodeInfo := DynamoNode{
		Address: hostAddr,
		Port:    hostPort,
	}
	return DynamoServer{
		wValue:         w,
		rValue:         r,
		preferenceList: preferenceList,
		selfNode:       selfNodeInfo,
		nodeID:         id,
		crash: false,
		objectMap: make(map[string]ObjectEntry)}
}

func ServeDynamoServer(dynamoServer DynamoServer) error {
	rpcServer := rpc.NewServer()
	e := rpcServer.RegisterName("MyDynamo", &dynamoServer)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Name Registration")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Registered the RPC Interfaces")

	l, e := net.Listen("tcp", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Port Listening")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Listening to Target Port ", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	log.Println(DYNAMO_SERVER, "Serving Server Now")

	return http.Serve(l, rpcServer)
}
