package readers

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"strings"

	log "github.com/gobs/logger"
)

var logger = log.GetLogger(log.INFO, "READERS")

// SetLogLevel set the logging level for readers
func SetLogLevel(level log.LogLevel) {
	logger.Level = level
}

// An InputReader is a "list" of lines (backed by a channel)
type InputReader chan string

// ArgReader returns a "list" of the input arguments
func ArgReader(args []string) chan string {
	ch := make(chan string)

	go func() {
		for _, arg := range args {
			ch <- arg
		}

		close(ch)
	}()

	return ch
}

// LineReader returns a "list" of lines read from the file.
// note that it skips empty lines
func LineReader(r io.Reader) chan string {
	ch := make(chan string)

	go func() {
		scanner := bufio.NewScanner(r)

		for scanner.Scan() {
			l := scanner.Text()
			l = strings.TrimSpace(l)
			if len(l) == 0 {
				continue
			}
			ch <- l
		}

		close(ch)
	}()

	return ch
}

// SQLReader returns a "list" of results from the specified queury
func SQLReader(db *sql.DB, query string) chan string {
	ch := make(chan string, 10)

	rows, err := db.Query(query)
	if err != nil {
		logger.Error("Query error: %s", err.Error())
		return nil
	}

	go func() {
		defer rows.Close()

		for rows.Next() {
			var v interface{}
			err = rows.Scan(&v)
			if err != nil {
				logger.Warning("Scan: %s", err.Error())
				continue
			}

			ch <- fmt.Sprintf("%v", v)
		}

		close(ch)
	}()

	return ch
}
