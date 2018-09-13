package workers

// Stack of values
type Stack struct {
	root   *node
	length uint
}

type node struct {
	value uint
	next  *node
}

// NewStack returns a new instance of our stack
func NewStack() *Stack {
	return &Stack{nil, 0}
}

// Length returns the number of elements currently on our stack
func (s *Stack) Length() uint {
	return s.length
}

// Push adds a new int value to the stack
func (s *Stack) Push(val uint) {
	n := &node{value: val, next: s.root}
	s.root = n
	s.length++
}

// Pop removes the top of the stack and returns the value
func (s *Stack) Pop() (uint, bool) {
	if s.length == 0 {
		return 0, false
	}

	val := s.root.value
	s.root = s.root.next
	s.length--
	return val, true
}
