package mydynamo

type VectorClock struct {
	//todo
	Count map[string]int // {nodeID : version}
}

//Creates a new VectorClock
func NewVectorClock() VectorClock {
	count := make(map[string]int)
	vc := VectorClock{ Count: count}
	return vc
}

//Returns true if the other VectorClock is causally descended from this one
func (s VectorClock) LessThan(otherClock VectorClock) bool {
	for nodeID, version := range s.Count{
		if version > otherClock.Count[nodeID]{
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
	s.Count[nodeId] += 1
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
func (s *VectorClock) Combine(clocks []VectorClock) {
	panic("todo")
}

//Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherClock VectorClock) bool {
	for nodeID, version := range s.Count {
		if version != otherClock.Count[nodeID]{
			return false
		}
	}
	return true
}
