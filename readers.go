package readers

import (
	"bufio"
	"database/sql"
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
	ch := make(chan string)

	rows, err := db.Query(query)
	if err != nil {
		logger.Error("Query error: %s", err.Error())
		return nil
	}

	go func() {
		defer rows.Close()
		var vp []interface{}
		var vals []string

		for rows.Next() {
			var v string

			cols, err := rows.Columns()
			if err != nil {
				logger.Fatal("rows.Columns: %v", err)
			}

			l := len(cols)

			if l == 1 {
				err = rows.Scan(&v)
				if err != nil {
					logger.Warning("Scan: %s", err.Error())
					continue
				}
			} else {
				if vals == nil {
					vp = make([]interface{}, l)
					vals = make([]string, l)
					for i := 0; i < l; i++ {
						vp[i] = &vals[i]
					}
				}
				err = rows.Scan(vp...)
				if err != nil {
					logger.Warning("Scan: %s", err.Error())
					continue
				}

				v = strings.Join(vals, " ")
			}

			ch <- v
		}

		close(ch)
	}()

	return ch
}
