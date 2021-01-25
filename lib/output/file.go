package output

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/bloblang"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeFile] = TypeSpec{
		constructor: fromSimpleConstructor(NewFile),
		Summary: `
Writes messages to files on disk based on a chosen codec.`,
		Description: `
Messages can be written to different files by using [interpolation functions](/docs/configuration/interpolation#bloblang-queries) in the path field. However, only one file is ever open at a given time, and therefore when the path changes the previously open file is closed.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"path", "The file to write to, if the file does not yet exist it will be created.",
				"/tmp/data.txt",
				"/tmp/${! timestamp_unix() }.txt",
				`/tmp/${! json("document.id") }.json`,
			).SupportsInterpolation(true).AtVersion("3.33.0"),
			codec.WriterDocs.AtVersion("3.33.0"),
			docs.FieldDeprecated("delimiter"),
		},
		Categories: []Category{
			CategoryLocal,
		},
	}
}

//------------------------------------------------------------------------------

// FileConfig contains configuration fields for the file based output type.
type FileConfig struct {
	Path  string `json:"path" yaml:"path"`
	Codec string `json:"codec" yaml:"codec"`
	Delim string `json:"delimiter" yaml:"delimiter"`
}

// NewFileConfig creates a new FileConfig with default values.
func NewFileConfig() FileConfig {
	return FileConfig{
		Path:  "",
		Codec: "lines",
		Delim: "",
	}
}

//------------------------------------------------------------------------------

// NewFile creates a new File output type.
func NewFile(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	if len(conf.File.Delim) > 0 {
		conf.File.Codec = "delim:" + conf.File.Delim
	}
	f, err := newFileWriter(conf.File.Path, conf.File.Codec, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncWriter(TypeFile, 1, f, log, stats)
}

//------------------------------------------------------------------------------

type fileWriter struct {
	log   log.Modular
	stats metrics.Type

	path      bloblang.Field
	codec     codec.WriterConstructor
	codecConf codec.WriterConfig

	handleMut  sync.Mutex
	handlePath string
	handle     codec.Writer
}

func newFileWriter(pathStr string, codecStr string, log log.Modular, stats metrics.Type) (*fileWriter, error) {
	codec, codecConf, err := codec.GetWriter(codecStr)
	if err != nil {
		return nil, err
	}
	path, err := bloblang.NewField(pathStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %w", err)
	}
	return &fileWriter{
		codec:     codec,
		codecConf: codecConf,
		path:      path,
		log:       log,
		stats:     stats,
	}, nil
}

//------------------------------------------------------------------------------

func (w *fileWriter) ConnectWithContext(ctx context.Context) error {
	return nil
}

func (w *fileWriter) WriteWithContext(ctx context.Context, msg types.Message) error {
	err := writer.IterateBatchedSend(msg, func(i int, p types.Part) error {
		path := filepath.Clean(w.path.String(i, msg))

		w.handleMut.Lock()
		defer w.handleMut.Unlock()

		if w.handle != nil && path == w.handlePath {
			return w.handle.Write(ctx, p)
		}
		if w.handle != nil {
			if err := w.handle.Close(ctx); err != nil {
				return err
			}
		}

		flag := os.O_CREATE | os.O_RDWR
		if w.codecConf.Append {
			flag = flag | os.O_APPEND
		}
		if w.codecConf.Truncate {
			flag = flag | os.O_TRUNC
		}

		if err := os.MkdirAll(filepath.Dir(path), os.FileMode(0777)); err != nil {
			return err
		}

		file, err := os.OpenFile(path, flag, os.FileMode(0666))
		if err != nil {
			return err
		}

		w.handlePath = path
		handle, err := w.codec(file)
		if err != nil {
			return err
		}

		if err = handle.Write(ctx, p); err != nil {
			handle.Close(ctx)
			return err
		}

		if !w.codecConf.CloseAfter {
			w.handle = handle
		} else {
			handle.Close(ctx)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if msg.Len() > 1 {
		w.handleMut.Lock()
		if w.handle != nil {
			w.handle.EndBatch()
		}
		w.handleMut.Unlock()
	}
	return nil
}

// CloseAsync shuts down the File output and stops processing messages.
func (w *fileWriter) CloseAsync() {
	go func() {
		w.handleMut.Lock()
		if w.handle != nil {
			w.handle.Close(context.Background())
			w.handle = nil
		}
		w.handleMut.Unlock()
	}()
}

// WaitForClose blocks until the File output has closed down.
func (w *fileWriter) WaitForClose(timeout time.Duration) error {
	return nil
}
