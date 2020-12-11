package mydynamo


type VectorClock struct {
	//todo
	CountMap map[string]int  // {nodeID : version}
}

func max(a, b int) int {
    if a < b {
        return b
    }
    return a
}

//Creates a new VectorClock
func NewVectorClock() VectorClock {
	count := make(map[string]int)
	vc := VectorClock{CountMap: count}
	return vc
}

//Returns true if the other VectorClock is causally descended from this one
func (s VectorClock) LessThan(otherClock VectorClock) bool {
	for nodeID, version := range s.CountMap{
		if version > otherClock.CountMap[nodeID]{
			return false
		}
	}
	return true
}

//Returns true if neither VectorClock is causally descended from the other
func (s VectorClock) Concurrent(otherClock VectorClock) bool {
	if !s.LessThan(otherClock) && !otherClock.LessThan(s) {
		return true
	}
	return false
}

//Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeId string) {
	(*s).CountMap[nodeId] += 1
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
func (s *VectorClock) Combine(clocks []VectorClock) {
	var vc VectorClock
	for i := 0; i < len(clocks); i++ {
		vc = clocks[i]
		for nodeID, version := range vc.CountMap {
			(*s).CountMap[nodeID] = max ((*s).CountMap[nodeID], version)
		}
	}
	for nodeID, _ := range s.CountMap {
		(*s).CountMap[nodeID] = (*s).CountMap[nodeID] + 1
	}
}

//Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherClock VectorClock) bool {
	for nodeID, version := range s.CountMap {
		if version != otherClock.CountMap[nodeID]{
			return false
		}
	}
	return true
}
