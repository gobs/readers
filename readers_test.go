package readers

import (
	"bytes"
	"testing"
)

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

func TestArgsReader(test *testing.T) {
	test_output := []string{}

	for v := range ArgReader(test_input) {
		test_output = append(test_output, v)
	}

	validate(test, test_input, test_output)
}

func TestLineReader(test *testing.T) {
	test_output := []string{}

	binput := bytes.NewBufferString("")
	for _, l := range test_input {
		binput.WriteString(l)
		binput.WriteString("\n")
	}

	for v := range LineReader(binput) {
		test_output = append(test_output, v)
	}

	validate(test, test_input, test_output)
}
