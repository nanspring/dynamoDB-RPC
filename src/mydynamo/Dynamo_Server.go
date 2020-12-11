package mydynamo

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
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
	var i int 
	var err_flag bool
	for key, nodes := range (*s).notReplicated{
		object := (*s).objectMap[key]
		i = 0 
		for _, node := range nodes {
			err_flag = false
			serverAddr := node.Address + ":" + node.Port
			for j := 0 ; j< len(object); j++ {
				args := NewPutArgs(key, object[j].Context, object[j].Value)
				err := s.RPCPut(serverAddr, &args)
				if err != nil { // deal with crash case 
					log.Println(err)
					err_flag = true
				}
			}
			if err_flag{
				// for crash case, keep the node as not replicated and do it next time 
				nodes[i] = node
				i++
			}
		}
		s.notReplicated[key] = s.notReplicated[key][:i]
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
		s.objectMap[key] = append(s.objectMap[key],ObjectEntry{NewContext(NewVectorClock()),new_value})
		s.objectMap[key][0].Context.Clock.CountMap[nodeID] = 0 
	}
	*result = true
	var vc VectorClock
	sign_replace := false
	sign_concurrent := false
	i := 0
	object = (*s).objectMap[key]
	for _, x := range object {
		vc = x.Context.Clock
		if vc.LessThan(new_context.Clock){ //new context is casually descent from old context
			if !sign_replace{
				s.objectMap[key][i] = ObjectEntry{NewContext(new_context.Clock),new_value}
				sign_replace = true
				i++
			}
		}else {
			*result = false
			s.objectMap[key][i] = x
			i++
			if vc.Concurrent(new_context.Clock) {
				sign_concurrent = true
			}
		}
	}
	s.objectMap[key] = s.objectMap[key][:i]
	if sign_concurrent{
		s.objectMap[key] = append(s.objectMap[key],ObjectEntry{new_context,new_value})
	}
	s.objectMap[key][0].Context.Clock.Increment(s.nodeID) //first position is its own vector clock
	count := 0
	for i = 0 ; i < len(s.preferenceList); i++ {
		node := s.preferenceList[i]
		serverAddr := node.Address + ":" + node.Port
		this_serverAddr := s.selfNode.Address + ":" + s.selfNode.Port
		if serverAddr == this_serverAddr{
			count++
			continue
		}
		if count < wValue{
			args := NewPutArgs(key, object[0].Context, object[0].Value) // first position is its own
			err := s.RPCPut(serverAddr, &args)
			if err != nil {
				*result = false
				return err
			}else{
				count++
			}
		}else{
			node := s.preferenceList[i]
			s.notReplicated[key] = append(s.notReplicated[key], node)
		}
	}
	if count < wValue{
		*result = false
	}
	// PrintObjectMap(s.nodeID, s.objectMap)
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
	var sign bool
	if object == nil {
		return errors.New("value empty")
	}
	i := 0
	j := 0
	for i = 0 ; i < len(object); i++ {
		(*result).EntryList = append((*result).EntryList , object[i])
	}
	count := 0
	for i = 0 ; i < len(s.preferenceList); i++ {
		node := s.preferenceList[i]
		serverAddr := node.Address + ":" + node.Port
		this_serverAddr := s.selfNode.Address + ":" + s.selfNode.Port
		if serverAddr == this_serverAddr{
			count++
			continue
		   }
		if count < rValue{
			args := NewGetArgs(key, serverAddr)
			err := s.RPCGet(args, &node_result)
			if err != nil {
				return err
			}
			// PrintEntryList(s.nodeID,node_result.EntryList)
			for i = 0 ; i < len(node_result.EntryList); i++ {
				vc_i := node_result.EntryList[i].Context.Clock
				j = 0 
				sign = true
				for _, x := range (*result).EntryList {
					vc_x := x.Context.Clock
					if !vc_x.LessThan(vc_i){
						(*result).EntryList[j] = x
						j++
					}
					if vc_i.Equals(vc_x) || vc_i.LessThan(vc_x){
						sign = false
					}	
				}
				(*result).EntryList = (*result).EntryList[:j]
				if sign{
					(*result).EntryList = append((*result).EntryList, node_result.EntryList[i])
				}
			}	
			count++	
		}
	}
	// PrintEntryList(s.nodeID,(*result).EntryList)
	return nil
}


func (s *DynamoServer) RPCPut(serverAddr string, value *PutArgs) error {
	conn, e := rpc.DialHTTP("tcp", serverAddr)
	if e != nil {
		log.Println("rpc error: ",e)
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
	*result = true
	key := value.Key
	new_value := value.Value
	new_context := value.Context
	object := (*s).objectMap[key]
	nodeID := (*s).nodeID
	if object == nil { //meaning this is a new object
		s.objectMap[key] = append(s.objectMap[key],ObjectEntry{new_context,new_value})
		s.objectMap[key][0].Context.Clock.CountMap[nodeID] = 0 
		// s.objectMap[key][0].Context.Clock.Increment(s.nodeID)
		
	}else{
		 //first position is its own vector clock
		var vc VectorClock
		sign_replace := false
		sign_concurrent := false
		i := 0
		object = (*s).objectMap[key]
		for _, x := range object {
			vc = x.Context.Clock
			if vc.LessThan(new_context.Clock){ //new context is casually descent from old context
				if !sign_replace{
					// replace the context with new context only once
					s.objectMap[key][i] = ObjectEntry{NewContext(new_context.Clock),new_value}
					sign_replace = true
					i++
				}
				// else not save the old context 
			}else {
				// only save the new context if it concurrent with existing one
				*result = false
				s.objectMap[key][i] = x
				i++
				if vc.Concurrent(new_context.Clock) {
					sign_concurrent = true
				}
			}
			// only first i should be kept, others are garbage collect 
			s.objectMap[key] = s.objectMap[key][:i]
			if sign_concurrent{
				s.objectMap[key] = append(s.objectMap[key],ObjectEntry{new_context,new_value})
			}
		}
	}
	// PrintObjectMap(s.nodeID, s.objectMap)
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
		return errors.New("value empty")
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
