package stdltsv

import (
	"bufio"
	"context"
	"io"
	"iter"
	"os"

	. "github.com/takanoriyanagitani/go-ltsv2avro/util"
)

func ReaderToRawStrings(r io.Reader) IO[iter.Seq2[string, error]] {
	return func(_ context.Context) (iter.Seq2[string, error], error) {
		return func(yield func(string, error) bool) {
			var s *bufio.Scanner = bufio.NewScanner(r)

			for s.Scan() {
				var line string = s.Text()

				if !yield(line, nil) {
					return
				}
			}
		}, nil
	}
}

var StdinToRawStrings IO[iter.Seq2[string, error]] = ReaderToRawStrings(
	os.Stdin,
)
