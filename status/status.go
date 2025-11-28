package status

import (
	"errors"
	"fmt"
)

// ErrStatusUnknown is returned when an unknown status string is encountered.
var ErrStatusUnknown = errors.New("unknown status")

// Status represents the current state of a ticket.
type Status struct {
	slug string
}

// String returns the string representation of the status.
func (s Status) String() string {
	return s.slug
}

// MarshalJSON implements json.Marshaler interface.
func (s Status) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `"%s"`, s.slug), nil
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (s *Status) UnmarshalJSON(data []byte) error {
	str := string(data)
	if len(str) < 2 || str[0] != '"' || str[len(str)-1] != '"' {
		return errors.New("invalid JSON string")
	}

	status, err := FromString(str[1 : len(str)-1])
	if err != nil {
		return err
	}

	*s = status
	return nil
}

// Predefined status values.
var (
	Pending   = Status{slug: "pending"}
	Done      = Status{slug: "done"}
	Failed    = Status{slug: "failed"}
	Cancelled = Status{slug: "cancelled"}
)

// FromString converts a string to a Status.
// Returns ErrStatusUnknown if the string doesn't match any known status.
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
	default:
		return Status{}, errors.Join(ErrStatusUnknown, fmt.Errorf("unknown status: %s", s))
	}
}
