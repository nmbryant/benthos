package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDrop] = TypeSpec{
		constructor: fromSimpleConstructor(NewDrop),
		Summary: `
Drops all messages.`,
		Categories: []Category{
			CategoryUtility,
		},
	}
}

//------------------------------------------------------------------------------

// NewDrop creates a new Drop output type.
func NewDrop(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return NewWriter(
		TypeDrop, writer.NewDrop(conf.Drop, log, stats), log, stats,
	)
}

//------------------------------------------------------------------------------
