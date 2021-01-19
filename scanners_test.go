package readers

import (
	"bytes"
	"testing"
)

/* already declared in test_readers.go
var (
	test_input = []string{
		"one",
		"two",
		"three",
		"four",
		"five",
		"six",
		"seven"}
)

func validate(test *testing.T, in, out []string) {
	if len(in) != len(out) {
		test.Logf("output (%d) != input (%d)", len(out), len(in))
		test.Fail()
	}

	for i := range in {
		if in[i] != out[i] {
			test.Logf("%d: output (%v) != input (%v)", i, out[i], in[i])
			test.Fail()
		}
	}
}
*/

func TestScanArgs(test *testing.T) {
	test_output := []string{}

	scanner := ScanArgs(test_input)

	for scanner.Scan() {
		test_output = append(test_output, scanner.Text())
	}

	if scanner.Err() != nil {
		test.Log("scanner error", scanner.Err())
		test.Fail()
	}

	validate(test, test_input, test_output)
}

func TestScanReader(test *testing.T) {
	test_output := []string{}

	binput := bytes.NewBufferString("")
	for _, l := range test_input {
		binput.WriteString(l)
		binput.WriteString("\n")
	}

	scanner := ScanReader(binput)

	for scanner.Scan() {
		test_output = append(test_output, scanner.Text())
	}

	if scanner.Err() != nil {
		test.Log("scanner error", scanner.Err())
		test.Fail()
	}

	validate(test, test_input, test_output)
}
