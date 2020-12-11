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
	objectMap	   map[string][]ObjectEntry //store the put operation 
	notReplicated   map[string][]DynamoNode // not replicated node lists

}

type GetArgs struct {
	Key     string
	serverAddr string
}

func NewGetArgs(key string, serverAddr string) GetArgs {
	return GetArgs{
		Key:     key,
		serverAddr: serverAddr, 
	}
}


func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
	s.preferenceList = incomingList
	return nil
}

// Forces server to gossip
// As this method takes no arguments, we must use the Empty placeholder
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error {
	for key, nodes := range s.notReplicated{
		object := (*s).objectMap[key]
		for i := 0 ; i < len(nodes); i++ {
			node := nodes[i]
			serverAddr := node.Address + ":" + node.Port
			for j := 0 ; j< len(object); j++ {
				args := NewPutArgs(key, object[j].Context, object[j].Value)
				err := s.RPCPut(serverAddr, &args)
				if err != nil {
					continue
				}
			}
		}
		s.notReplicated[key] = make([]DynamoNode, 0)
	}
	return nil
}

//Makes server unavailable for some seconds
func (s *DynamoServer) Crash(seconds int, success *bool) error {
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
		*result = false
		return errors.New("node crash")
	}
	wValue := s.wValue 
	key := value.Key
	new_value := value.Value
	new_context := value.Context
	object := (*s).objectMap[key]
	nodeID := s.nodeID
	if object == nil { //meaning this is a new object
		s.objectMap[key] = make([]ObjectEntry, 0)
		s.objectMap[key] = append(s.objectMap[key],ObjectEntry{NewContext(NewVectorClock()),new_value})
		object[0].Context.Clock.countMap[nodeID] = 0 
	}
	object[0].Context.Clock.Increment(s.nodeID) //first position is its own vector clock
	*result = true
	var vc VectorClock
	sign_replace := false
	sign_concurrent := false
	i := 0
	for _, x := range object {
		vc = x.Context.Clock
		if vc.LessThan(new_context.Clock){ //new context is casually descent from old context
			if !sign_replace{
				object[i] = ObjectEntry{NewContext(new_context.Clock),new_value}
				sign_replace = true
				i++
			}
		}else {
			*result = false
			object[i] = x
			i++
			if vc.Concurrent(new_context.Clock) {
				sign_concurrent = true
			}
		}
	}
	object = object[:i]
	if sign_concurrent{
		s.objectMap[key] = append(s.objectMap[key],ObjectEntry{new_context,new_value})
	}
	count := 0
	for i = 0 ; i < len(s.preferenceList); i++ {
		if strconv.Itoa(i) == nodeID{
			count++
			continue
		}
		if count < wValue{
			node := s.preferenceList[i]
			serverAddr := node.Address + ":" + node.Port
			args := NewPutArgs(key, object[0].Context, object[0].Value)
			err := s.RPCPut(serverAddr, &args)
			if err != nil {
				*result = false
				return err
			}else{
				count++
			}
		}else{
			node := s.preferenceList[i]
			notReplicated_node := (*s).notReplicated[key]
			if notReplicated_node == nil{
				s.notReplicated[key] = make([]DynamoNode, 0)
			}
			s.notReplicated[key] = append(s.notReplicated[key], node)
		}
	}
	if count < wValue{
		*result = false
	}
	return nil
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
	if s.crash {
		return errors.New("node crash")
	}
	rValue := s.rValue 
	object := (*s).objectMap[key]
	var node_result DynamoResult
	if object == nil {
		return errors.New("Value Empty")
	}
	i := 0
	j := 0
	for i = 0 ; i < len(object); i++ {
		(*result).EntryList = append((*result).EntryList , object[i])
	}
	count := 0
	for i = 0 ; i < len(s.preferenceList); i++ {
		if strconv.Itoa(i) == s.nodeID{
			count++
			continue
		}
		if count < rValue{
			node := s.preferenceList[i]
			serverAddr := node.Address + ":" + node.Port
			args := NewGetArgs(key, serverAddr)
			err := s.RPCGet(args, &node_result)
			if err != nil {
				return err
			}
			for i = 0 ; i < len(node_result.EntryList); i++ {
				vc_i := node_result.EntryList[i].Context.Clock
				for j=0; j < len((*result).EntryList); j++ {
					vc_j := (*result).EntryList[j].Context.Clock
					if vc_i.Equals(vc_j) || vc_i.LessThan(vc_j){
						break
					}
				}
				(*result).EntryList = append((*result).EntryList, node_result.EntryList[i])
			}	
			count++	
		}
	}
	reserve_list := make([]bool, len((*result).EntryList))
	for i = 0 ; i < len((*result).EntryList); i++ {
		vc_i := (*result).EntryList[i].Context.Clock
		for j=i+1; j < len((*result).EntryList); j++ {
			vc_j := (*result).EntryList[j].Context.Clock
			if vc_i.LessThan(vc_j){
				reserve_list[i] = false
				break
			}
		}
		reserve_list[i] = true
	}
	i = 0 
	j = 0 
	for _, x := range (*result).EntryList {
		if reserve_list[j]{
			(*result).EntryList[i] = x
			i++
		}
		j++
	}
	(*result).EntryList = (*result).EntryList[:i]
	return nil
}


func (s *DynamoServer) RPCPut(serverAddr string, value *PutArgs) error {
	conn, e := rpc.DialHTTP("tcp", serverAddr)
	if e != nil {
		return e
	}
	// perform the call
	var success bool
	err := conn.Call("MyDynamo.PutToPreference", (*value), &success)
	if err != nil {
		conn.Close()
		return err
	}
	// close the connection
	return conn.Close()
}


func (s *DynamoServer) PutToPreference(value PutArgs, result *bool) error{
	if s.crash {
		*result = false
		return errors.New("node crash")
	}
	key := value.Key
	new_value := value.Value
	new_context := value.Context
	object := (*s).objectMap[key]
	nodeID := s.nodeID
	if object == nil { //meaning this is a new object
		s.objectMap[key] = make([]ObjectEntry, 0)
		s.objectMap[key] = append(s.objectMap[key],ObjectEntry{NewContext(NewVectorClock()),new_value})
		object[0].Context.Clock.countMap[nodeID] = 0 
	}
	object[0].Context.Clock.Increment(s.nodeID) //first position is its own vector clock
	*result = true
	var vc VectorClock
	sign_replace := false
	sign_concurrent := false
	i := 0
	for _, x := range object {
		vc = x.Context.Clock
		if vc.LessThan(new_context.Clock){ //new context is casually descent from old context
			if !sign_replace{
				object[i] = ObjectEntry{NewContext(new_context.Clock),new_value}
				sign_replace = true
				i++
			}
		}else {
			*result = false
			object[i] = x
			i++
			if vc.Concurrent(new_context.Clock) {
				sign_concurrent = true
			}
		}
	}
	object = object[:i]
	if sign_concurrent{
		s.objectMap[key] = append(s.objectMap[key],ObjectEntry{new_context,new_value})
	}
	return nil
}


func (s *DynamoServer) RPCGet(value GetArgs, result *DynamoResult) error {
	serverAddr := value.serverAddr
	key := value.Key
	conn, e := rpc.DialHTTP("tcp", serverAddr)
	if e != nil {
		return e
	}
	err := conn.Call("MyDynamo.GetFromPreference", key, result)
	if err != nil {
		return conn.Close()
	}
	// close the connection
	return conn.Close()
}


func (s *DynamoServer) GetFromPreference(key string, result *DynamoResult) error{
	if s.crash {
		return errors.New("node crash")
	}
	object := (*s).objectMap[key]
	if object == nil {
		return errors.New("Value Empty")
	}
	for i := 0 ; i < len(object); i++ {
		(*result).EntryList = append((*result).EntryList , object[i])
	}
	return nil
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
		objectMap: make(map[string][]ObjectEntry), 
		notReplicated: make(map[string][]DynamoNode)}
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
