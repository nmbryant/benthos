package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {

	var credentialsFields = docs.FieldSpecs{
		docs.FieldCommon("username", "The username to connect to the SFTP server."),
		docs.FieldCommon("secret", "The secret/password for the username to connect to the SFTP server."),
	}

	Constructors[TypeSFTP] = TypeSpec{
		constructor: NewSFTP,
		Status:      docs.StatusBeta,
		Version:     "3.36.0",
		Summary: `
Sends message parts as objects to a file via an SFTP connection.`,
		Description: `
In order to have a different path for each object you should use function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"server",
				"The server to connect to and save files on.",
			),
			docs.FieldCommon(
				"port",
				"The port to connect to on the server.",
			),
			docs.FieldCommon(
				"filepath",
				"The file to save the messages to on the server.",
			),
			docs.FieldCommon(
				"credentials",
				"The credentials to use to log into the server.",
			).WithChildren(credentialsFields...),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

// NewSFTP creates a new SFTP output type.
func NewSFTP(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	sftp, err := writer.NewSFTP(conf.SFTP, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncWriter(
		TypeSFTP, conf.SFTP.MaxInFlight, sftp, log, stats,
	)
}

//------------------------------------------------------------------------------