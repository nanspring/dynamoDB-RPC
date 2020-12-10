package mydynamo

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"strconv"
)

type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue         int          //Number of nodes to write to on each Put
	rValue         int          //Number of nodes to read from on each Get
	preferenceList []DynamoNode //Ordered list of other Dynamo nodes to perform operations o
	selfNode       DynamoNode   //This node's address and port info
	nodeID         string       //ID of this node
	crash          bool
	objectMap	   map[string][]ObjectEntry //store the put operation

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
	//panic("todo") 
	key := value.Key
	new_context := value.Context
	value := value.Value
	object := s.objectMap[key]
	if object == nil { //meaning this is a new object
		s.objectMap[key] = make([]ObjectEntry)
		s.objectMap[key] = append(s.objectMap[key],ObjectEntry{NewContext(NewVectorClock()),value})
		clock_adr := &s.objectMap[key][0].Context.Clock
		s.SetVectorClock(clock_adr,s.nodeID,len(s.preferenceList)) //[1,0,0]
	}else{ //this is an existing object
		old_context := object[0].Context
		if !old_context.Clock.LessThan(new_context.Clock) { //if new context is not casually descent from old context, put fail
			*result = false
			return nil
		}

		old_context.Clock.Increment(s.nodeID)

	}
	for i := 0 ; i < len(s.preferenceList); i++ {
		
	}
	


}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
	panic("todo")
}

//Set vector clock
func (s *DynamoServer) SetVectorClock(clock *VectorClock, nodeID string, size int){
	for i := 0; i < size; i++ {
		clock.countMap[strconv.Itoa(i)] = 0
	}
	clock.countMap[nodeID] = 1
}

func (s *DynamoServer) RPCPut(serverAddr, value PutArgs){
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
	return conn.Close()
}
func (s *DynamoServer) PutToPreference(value PutArgs, success *bool){
	if s.crash {
		*success = false
		return
	}
	key := value.Key
	new_context := value.Context
	value := value.Value
	object := s.objectMap[key]

	if object == nil { //meaning this is a new object
		s.objectMap[key] = ObjectEntry{NewContext(NewVectorClock()),value}
		clock_adr := &s.objectMap[key].Context.Clock
		s.SetVectorClock(clock_adr,s.nodeID,len(s.preferenceList))
	}else{ //this is an existing object
		old_context := object.Context
		if !old_context.Clock.LessThan(new_context.Clock) { //if new context is not casually descent from old context
			*success = false
			return nil
		}
		s.objectMap[key].Value = value
		s.objectMap[key].Context = new_context
	}
	

	*success = true

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
