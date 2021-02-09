package input

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/docs"
	sftpSetup "github.com/Jeffail/benthos/v3/internal/service/sftp"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/pkg/sftp"
)

func init() {
	Constructors[TypeSFTP] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			r, err := newSFTPReader(conf.SFTP, log, stats)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(
				TypeSFTP,
				true,
				reader.NewAsyncPreserver(r),
				log, stats,
			)
		}),
		Status:  docs.StatusExperimental,
		Version: "3.39.0",
		Summary: `Consumes files from a server over SFTP.`,
		Description: `
## Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- sftp_path
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"address",
				"The address of the server to connect to that has the target files.",
			),
			docs.FieldCommon(
				"credentials",
				"The credentials to use to log into the server.",
			).WithChildren(sftpSetup.CredentialsDocs()...),
			docs.FieldCommon(
				"paths",
				"A list of paths to consume sequentially. Glob patterns are supported.",
			),
			codec.ReaderDocs,
			docs.FieldAdvanced("delete_on_finish", "Whether to delete files from the server once they are processed."),
			docs.FieldAdvanced("max_buffer", "The largest token size expected when consuming delimited files."),
			docs.FieldCommon(
				"watcher_mode",
				"Whether it keeps running after processing all the files in the paths to watch for new files.",
			),
		},
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

// SFTPConfig contains configuration fields for the SFTP input type.
type SFTPConfig struct {
	Address        string                `json:"address" yaml:"address"`
	Credentials    sftpSetup.Credentials `json:"credentials" yaml:"credentials"`
	Paths          []string              `json:"paths" yaml:"paths"`
	Codec          string                `json:"codec" yaml:"codec"`
	DeleteOnFinish bool                  `json:"delete_on_finish" yaml:"delete_on_finish"`
	MaxBuffer      int                   `json:"max_buffer" yaml:"max_buffer"`
	WatcherMode    bool                  `json:"watcher_mode" yaml:"watcher_mode"`
}

// NewSFTPConfig creates a new SFTPConfig with default values.
func NewSFTPConfig() SFTPConfig {
	return SFTPConfig{
		Address:        "",
		Credentials:    sftpSetup.Credentials{},
		Paths:          []string{},
		Codec:          "all-bytes",
		DeleteOnFinish: false,
		MaxBuffer:      1000000,
		WatcherMode:    false,
	}
}

//------------------------------------------------------------------------------

type sftpReader struct {
	conf SFTPConfig

	log   log.Modular
	stats metrics.Type

	client *sftp.Client

	paths          []string
	filesProcessed map[string]struct{}
	scannerCtor    codec.ReaderConstructor

	scannerMut  sync.Mutex
	scanner     codec.Reader
	currentPath string
}

func newSFTPReader(conf SFTPConfig, log log.Modular, stats metrics.Type) (*sftpReader, error) {
	codecConf := codec.NewReaderConfig()
	codecConf.MaxScanTokenSize = conf.MaxBuffer
	ctor, err := codec.GetReader(conf.Codec, codecConf)
	if err != nil {
		return nil, err
	}

	s := &sftpReader{
		conf:           conf,
		log:            log,
		stats:          stats,
		scannerCtor:    ctor,
		filesProcessed: map[string]struct{}{},
	}

	return s, err
}

// ConnectWithContext attempts to establish a connection to the target SFTP server.
func (s *sftpReader) ConnectWithContext(ctx context.Context) error {
	s.scannerMut.Lock()
	defer s.scannerMut.Unlock()

	if s.scanner != nil {
		return nil
	}

	if s.client == nil {
		var err error
		if s.client, err = s.conf.Credentials.GetClient(s.conf.Address); err != nil {
			return err
		}
		s.paths = s.getFilePaths()
	}

	if len(s.paths) == 0 {
		if !s.conf.WatcherMode {
			s.client.Close()
			s.client = nil
			return types.ErrTypeClosed
		} else {
			s.paths = s.getFilePaths()
			return nil
		}
	}

	nextPath := s.paths[0]

	file, err := s.client.Open(nextPath)
	if err != nil {
		return err
	}

	if s.scanner, err = s.scannerCtor(nextPath, file, func(ctx context.Context, err error) error {
		if err == nil && s.conf.DeleteOnFinish {
			return s.client.Remove(nextPath)
		}
		return nil
	}); err != nil {
		file.Close()
		return err
	}

	s.currentPath = nextPath
	s.paths = s.paths[1:]

	s.log.Infof("Consuming from file '%v'\n", nextPath)
	return err
}

// ReadWithContext attempts to read a new message from the target file(s) on the server.
func (s *sftpReader) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	s.scannerMut.Lock()
	defer s.scannerMut.Unlock()

	if s.scanner == nil || s.client == nil {
		return nil, nil, types.ErrNotConnected
	}

	part, codecAckFn, err := s.scanner.Next(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			err = types.ErrTimeout
		}
		if err != types.ErrTimeout {
			s.filesProcessed[s.currentPath] = struct{}{}
			s.scanner.Close(ctx)
			s.scanner = nil
		}
		if errors.Is(err, io.EOF) {
			err = types.ErrTimeout
		}
		return nil, nil, err
	}

	part.Metadata().Set("sftp_path", s.currentPath)
	msg := message.New(nil)
	msg.Append(part)

	return msg, func(ctx context.Context, res types.Response) error {
		return codecAckFn(ctx, res.Error())
	}, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (s *sftpReader) CloseAsync() {
	go func() {
		s.scannerMut.Lock()
		if s.scanner != nil {
			s.scanner.Close(context.Background())
			s.scanner = nil
			s.paths = nil
		}
		if s.client != nil {
			s.client.Close()
			s.client = nil
		}
		s.scannerMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (s *sftpReader) WaitForClose(timeout time.Duration) error {
	return nil
}

func (s *sftpReader) getFilePaths() []string {
	var filepaths []string
	for _, p := range s.conf.Paths {
		paths, err := s.client.Glob(p)
		if err != nil {
			s.log.Warnf("Failed to scan files from path %v: %v\n", p, err)
			continue
		}
		for _, path := range paths {
			if _, ok := s.filesProcessed[path]; !ok {
				filepaths = append(filepaths, path)
			}
		}
	}
	return filepaths
}
