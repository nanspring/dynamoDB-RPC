package mydynamotest

import (
    "mydynamo"
    "os/exec"
    "log"
    "os"
    "strconv"
)


func InitDynamoServer(config_path string) *exec.Cmd {
    serverCmd := exec.Command("DynamoCoordinator", config_path)
    serverCmd.Stderr = os.Stderr
    serverCmd.Stdout = os.Stdout
    return serverCmd
}


func StartDynamoServer(server *exec.Cmd, ready chan bool) {
    err := server.Start()
    if err != nil {
        log.Println(err)
    }

    ready <- true
}

func KillDynamoServer(server *exec.Cmd) {
    _ = server.Process.Kill()
    exec.Command("pkill SurfstoreServerExec*")
}

func MakeConnectedClient(port int) *mydynamo.RPCClient{
    clientInstance := mydynamo.NewDynamoRPCClient("localhost:" + strconv.Itoa(port))
    clientInstance.RpcConnect()
    return clientInstance
}

func PutFreshContext(key string, value []byte) mydynamo.PutArgs{
    return mydynamo.NewPutArgs(key, mydynamo.NewContext(mydynamo.NewVectorClock()), value)
}

func valuesEqual(v1 []byte, v2 []byte) bool {
    if(len(v1) != len(v2)){
        return false
    }
    for idx, b := range(v1){
        if b != v2[idx]{
            return false
        }
    }
    return true
}

func GetEntryByVectorClock(result mydynamo.DynamoResult, desiredClock mydynamo.VectorClock) int{
    for idx, entry := range(result.EntryList){
        if(entry.Context.Clock.Equals(desiredClock) == true){
            return idx
        }
    }
    return -1
}
