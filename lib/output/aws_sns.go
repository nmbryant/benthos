package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAWSSNS] = TypeSpec{
		constructor: fromSimpleConstructor(NewAWSSNS),
		Version:     "3.36.0",
		Summary: `
Sends messages to an AWS SNS topic.`,
		Description: `
### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("topic_arn", "The topic to publish to."),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldAdvanced("timeout", "The maximum period to wait on an upload before abandoning it and reattempting."),
		}.Merge(session.FieldSpecs()),
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
		},
	}

	Constructors[TypeSNS] = TypeSpec{
		constructor: fromSimpleConstructor(NewAmazonSNS),
		Status:      docs.StatusDeprecated,
		Summary: `
Sends messages to an AWS SNS topic.`,
		Description: `
## Alternatives

This output has been renamed to ` + "[`aws_sns`](/docs/components/outputs/aws_sns)" + `.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("topic_arn", "The topic to publish to."),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldAdvanced("timeout", "The maximum period to wait on an upload before abandoning it and reattempting."),
		}.Merge(session.FieldSpecs()),
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
		},
	}
}

//------------------------------------------------------------------------------

// NewAWSSNS creates a new AmazonSNS output type.
func NewAWSSNS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return newAmazonSNS(TypeAWSSNS, conf.AWSSNS, mgr, log, stats)
}

// NewAmazonSNS creates a new AmazonSNS output type.
func NewAmazonSNS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return newAmazonSNS(TypeSNS, conf.SNS, mgr, log, stats)
}

func newAmazonSNS(name string, conf writer.SNSConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	s, err := writer.NewSNS(conf, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.MaxInFlight == 1 {
		return NewWriter(name, s, log, stats)
	}
	return NewAsyncWriter(name, conf.MaxInFlight, s, log, stats)
}

//------------------------------------------------------------------------------
