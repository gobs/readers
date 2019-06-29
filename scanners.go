package readers

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"strings"
)

// An InputScanner returns a list of lines
type InputScanner interface {
	Scan() bool
	Text() string
	Err() error
}

// ArgScanner scan a list of arguments
type ArgScanner struct {
	args []string
	curr string
}

// ScanArgs returns an ArgScanner
func ScanArgs(args []string) InputScanner {
	return &ArgScanner{args: args}
}

func (s *ArgScanner) Scan() bool {
	if len(s.args) == 0 {
		s.curr = ""
		return false
	}

	s.curr, s.args = s.args[0], s.args[1:]
	return true
}

func (s *ArgScanner) Text() string {
	return s.curr
}

func (s *ArgScanner) Err() error {
	return nil
}

type ReaderScanner struct {
	scanner *bufio.Scanner
	curr    string
}

// ScanReader returns an ReaderScanner
func ScanReader(r io.Reader) InputScanner {
	return &ReaderScanner{scanner: bufio.NewScanner(r)}
}

func (s *ReaderScanner) Scan() bool {
	for s.scanner.Scan() {
		s.curr = s.scanner.Text()
		s.curr = strings.TrimSpace(s.curr)
		if len(s.curr) == 0 {
			continue
		}
		return true
	}

	return false
}

func (s *ReaderScanner) Text() string {
	return s.curr
}

func (s *ReaderScanner) Err() error {
	return s.scanner.Err()
}

type SQLScanner struct {
	rows *sql.Rows
	err  error
	curr string
}

// SQLScanner returns an InputScanner running over the result of the specified SQL query
func ScanSQL(db *sql.DB, query string) InputScanner {
	rows, err := db.Query(query)
	if err != nil {
		logger.Error("Query error: %s", err.Error())
		return nil
	}

	return &SQLScanner{rows: rows, err: err}
}

func (s *SQLScanner) Scan() bool {
	more := s.rows.Next()
	if !more {
		s.rows.Close()
		return false
	}

	var v interface{}
	s.err = s.rows.Scan(&v)
	if s.err != nil {
		logger.Warning("Scan: %s", s.err.Error())
		s.curr = ""
	} else {
		s.curr = fmt.Sprintf("%v", v)
	}

	return true
}

func (s *SQLScanner) Text() string {
	return s.curr
}

func (s *SQLScanner) Err() error {
	return s.err
}
