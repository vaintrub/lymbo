package status

import (
	"errors"
	"fmt"
)

var ErrStatusUnknown = errors.New("unknown status")

type Status struct {
	slug string
}

func (s Status) String() string {
	return s.slug
}

func (s Status) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `"%s"`, s.slug), nil
}

func (s *Status) UnmarshalJSON(data []byte) error {
	str := string(data)
	if len(str) < 2 || str[0] != '"' || str[len(str)-1] != '"' {
		return errors.New("status: invalid JSON string")
	}
	status, err := FromString(str[1 : len(str)-1])
	if err != nil {
		return err
	}
	*s = status
	return nil
}

var (
	Pending   = Status{slug: "pending"}
	Done      = Status{slug: "done"}
	Failed    = Status{slug: "failed"}
	Cancelled = Status{slug: "cancelled"}
)

func FromString(s string) (Status, error) {
	switch s {
	case Pending.slug:
		return Pending, nil
	case Done.slug:
		return Done, nil
	case Failed.slug:
		return Failed, nil
	case Cancelled.slug:
		return Cancelled, nil
	}
	return Status{}, errors.Join(ErrStatusUnknown, fmt.Errorf("unknown status: %s", s))
}
