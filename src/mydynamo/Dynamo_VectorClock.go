package mydynamo

type VectorClock struct {
	//todo
}

//Creates a new VectorClock
func NewVectorClock() VectorClock {
	panic("todo")
}

//Returns true if the other VectorClock is causally descended from this one
func (s VectorClock) LessThan(otherClock VectorClock) bool {
	panic("todo")
}

//Returns true if neither VectorClock is causally descended from the other
func (s VectorClock) Concurrent(otherClock VectorClock) bool {
	panic("todo")
}

//Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeId string) {
	panic("todo")
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
func (s *VectorClock) Combine(clocks []VectorClock) {
	panic("todo")
}

//Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherClock VectorClock) bool {
	panic("todo")
}
