package manager

import "errors"

var ErrTaskNotFound = errors.New("task not found")
var ErrTaskEventAlreadyExists = errors.New("task event already exists")
