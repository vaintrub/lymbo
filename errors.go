package lymbo

import "errors"

var ErrLimitInvalid = errors.New("limit is invalid")
var ErrTicketIDEmpty = errors.New("ticket ID is empty")
var ErrTicketNotFound = errors.New("ticket not found")
var ErrInvalidStatusTransition = errors.New("invalid status transition")
