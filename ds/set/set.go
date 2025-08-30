package set

type Set[T comparable] struct {
	data map[T]struct{}
}

func New[T comparable]() *Set[T] {
	return &Set[T]{data: make(map[T]struct{})}
}

func (s *Set[T]) Add(value T) {
	s.data[value] = struct{}{}
}

func (s *Set[T]) Remove(value T) {
	delete(s.data, value)
}

func (s *Set[T]) Contains(value T) bool {
	_, ok := s.data[value]
	return ok
}

func (s *Set[T]) Len() int {
	return len(s.data)
}
