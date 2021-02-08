package codec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// WriterDocs is a static field documentation for output codecs.
var WriterDocs = docs.FieldCommon(
	"codec", "The way in which the bytes of messages should be written out into the output file. It's possible to write lines using a custom delimiter with the `delim:x` codec, where x is the character sequence custom delimiter.", "lines", "delim:\t", "delim:foobar",
).HasAnnotatedOptions(
	"all-bytes", "Write the message to the file in full. If the file already exists the old content is deleted.",
	"append", "Append messages to the file.",
	"lines", "Append messages to the file followed by a line break.",
	"delim:x", "Append messages to the file followed by a custom delimiter.",
)

//------------------------------------------------------------------------------

// Writer is a codec type that reads message parts from a source.
type Writer interface {
	Write(context.Context, types.Part) error
	Close(context.Context) error

	// TODO V4: Remove this, we only have it in place in order to satisfy the
	// relatively dodgy empty line at end of batch behaviour.
	EndBatch() error
}

// WriterConfig contains custom configuration specific to a codec describing how
// handles should be provided.
type WriterConfig struct {
	Append     bool
	Truncate   bool
	CloseAfter bool
}

// WriterConstructor creates a writer from an io.WriteCloser.
type WriterConstructor func(io.WriteCloser) (Writer, error)

// GetWriter returns a constructor that creates write codecs.
func GetWriter(codec string) (WriterConstructor, WriterConfig, error) {
	switch codec {
	case "all-bytes":
		return func(w io.WriteCloser) (Writer, error) {
			return &allBytesWriter{w}, nil
		}, allBytesConfig, nil
	case "append":
		return func(w io.WriteCloser) (Writer, error) {
			return newCustomDelimWriter(w, "")
		}, customDelimConfig, nil
	case "lines":
		return newLinesWriter, linesWriterConfig, nil
	}
	if strings.HasPrefix(codec, "delim:") {
		by := strings.TrimPrefix(codec, "delim:")
		if len(by) == 0 {
			return nil, WriterConfig{}, errors.New("custom delimiter codec requires a non-empty delimiter")
		}
		return func(w io.WriteCloser) (Writer, error) {
			return newCustomDelimWriter(w, by)
		}, customDelimConfig, nil
	}
	return nil, WriterConfig{}, fmt.Errorf("codec was not recognised: %v", codec)
}

//------------------------------------------------------------------------------

var allBytesConfig = WriterConfig{
	Truncate:   true,
	CloseAfter: true,
}

type allBytesWriter struct {
	o io.WriteCloser
}

func (a *allBytesWriter) Write(ctx context.Context, msg types.Part) error {
	_, err := a.o.Write(msg.Get())
	return err
}

func (a *allBytesWriter) EndBatch() error {
	return nil
}

func (a *allBytesWriter) Close(ctx context.Context) error {
	return a.o.Close()
}

//------------------------------------------------------------------------------

var linesWriterConfig = WriterConfig{
	Append: true,
}

type linesWriter struct {
	w io.WriteCloser
}

func newLinesWriter(w io.WriteCloser) (Writer, error) {
	return &linesWriter{w: w}, nil
}

func (l *linesWriter) Write(ctx context.Context, p types.Part) error {
	if _, err := l.w.Write(p.Get()); err != nil {
		return err
	}
	_, err := l.w.Write([]byte("\n"))
	return err
}

func (l *linesWriter) EndBatch() error {
	_, err := l.w.Write([]byte("\n"))
	return err
}

func (l *linesWriter) Close(ctx context.Context) error {
	return l.w.Close()
}

//------------------------------------------------------------------------------

var customDelimConfig = WriterConfig{
	Append: true,
}

type customDelimWriter struct {
	w     io.WriteCloser
	delim []byte
}

func newCustomDelimWriter(w io.WriteCloser, delim string) (Writer, error) {
	delimBytes := []byte(delim)
	return &customDelimWriter{w: w, delim: delimBytes}, nil
}

func (d *customDelimWriter) Write(ctx context.Context, p types.Part) error {
	if _, err := d.w.Write(p.Get()); err != nil {
		return err
	}
	_, err := d.w.Write(d.delim)
	return err
}

func (d *customDelimWriter) EndBatch() error {
	_, err := d.w.Write(d.delim)
	return err
}

func (d *customDelimWriter) Close(ctx context.Context) error {
	return d.w.Close()
}
