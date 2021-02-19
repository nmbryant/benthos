package processor

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	bmongo "github.com/Jeffail/benthos/v3/internal/service/mongodb"
	"github.com/Jeffail/benthos/v3/lib/bloblang"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/cenkalti/backoff/v4"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

var documentMapOps = map[string]bool{
	"insert-one":  true,
	"delete-one":  false,
	"delete-many": false,
	"replace-one": true,
	"update-one":  true,
	"find-one":    false,
}

var filterMapOps = map[string]bool{
	"insert-one":  false,
	"delete-one":  true,
	"delete-many": true,
	"replace-one": true,
	"update-one":  true,
	"find-one":    true,
}

var hintAllowedOps = map[string]bool{
	"insert-one":  false,
	"delete-one":  true,
	"delete-many": true,
	"replace-one": true,
	"update-one":  true,
	"find-one":    true,
}

//------------------------------------------------------------------------------

func init() {

	Constructors[TypeMongoDB] = TypeSpec{
		constructor: NewMongoDB,
		Categories: []Category{
			CategoryIntegration,
		},
		Summary: `
Performs operations against a mongo db for each message, 
allowing you to store or retrieve data within message payloads.`,
		Description: ``,
		FieldSpecs: bmongo.ConfigDocs().Add(
			docs.FieldCommon(
				"operation",
				"The mongodb operation to perform. Must be one of the following: insert-one, delete-one, delete-many, "+
					"replace-one, update-one, find-one.",
			),
			docs.FieldCommon(
				"write_concern",
				"The write_concern settings for the mongo connection.",
			).WithChildren(bmongo.WriteConcernDocs()...),
			docs.FieldCommon(
				"document_map",
				"A bloblang map representing the records in the mongo db.",
			),
			docs.FieldCommon(
				"filter_map",
				"A bloblang map representing the filter for the mongo db command.",
			),
			docs.FieldCommon(
				"hint_map",
				"A bloblang map representing the hint for the mongo db command.",
			),
			partsFieldSpec,
		).Merge(retries.FieldSpecs()),
	}
}

//------------------------------------------------------------------------------

// MongoDBConfig contains configuration fields for the MongoDB processor.
type MongoDBConfig struct {
	MongoDB      bmongo.Config       `json:",inline" yaml:",inline"`
	WriteConcern bmongo.WriteConcern `json:"write_concern" yaml:"write_concern"`

	Parts       []int          `json:"parts" yaml:"parts"`
	Operation   string         `json:"operation" yaml:"operation"`
	FilterMap   string         `json:"filter_map" yaml:"filter_map"`
	DocumentMap string         `json:"document_map" yaml:"document_map"`
	HintMap     string         `json:"hint_map" yaml:"hint_map"`
	RetryConfig retries.Config `json:",inline" yaml:",inline"`
}

// NewMongoDBConfig returns a MongoDBConfig with default values.
func NewMongoDBConfig() MongoDBConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 3
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"

	return MongoDBConfig{
		MongoDB:      bmongo.NewConfig(),
		Parts:        []int{},
		Operation:    "insert",
		RetryConfig:  rConf,
		WriteConcern: bmongo.WriteConcern{},
	}
}

//------------------------------------------------------------------------------

// MongoDB is a processor that stores or retrieves data from a mongo db for each
// message of a batch
type MongoDB struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	client     *mongo.Client
	collection *mongo.Collection

	filterMap   bloblang.Mapping
	documentMap bloblang.Mapping
	hintMap     bloblang.Mapping

	parts []int

	mu          sync.Mutex
	backoffCtor func() backoff.BackOff

	boffPool sync.Pool

	closedChan <-chan struct{}
	runningCh  chan struct{}
	closingCh  chan struct{}
	closeOnce  sync.Once

	mCount            metrics.StatCounter
	mErr              metrics.StatCounter
	mKeyAlreadyExists metrics.StatCounter
	mSent             metrics.StatCounter
	mBatchSent        metrics.StatCounter
}

// NewMongoDB returns a MongoDB processor.
func NewMongoDB(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	m := &MongoDB{
		conf:  conf,
		log:   log,
		stats: stats,

		parts: conf.MongoDB.Parts,

		mCount:            stats.GetCounter("count"),
		mErr:              stats.GetCounter("error"),
		mKeyAlreadyExists: stats.GetCounter("key_already_exists"),
		mSent:             stats.GetCounter("sent"),
		mBatchSent:        stats.GetCounter("batch.sent"),
	}

	if conf.MongoDB.Operation == "" {
		return nil, fmt.Errorf("operator is a required field")
	}

	if conf.MongoDB.MongoDB.URL == "" {
		return nil, errors.New("mongo url must be specified")
	}

	if conf.MongoDB.MongoDB.Database == "" {
		return nil, errors.New("mongo database must be specified")
	}

	if conf.MongoDB.MongoDB.Collection == "" {
		return nil, errors.New("mongo collection must be specified")
	}

	var filterNeeded, documentNeeded bool
	var hintAllowed bool

	if _, ok := documentMapOps[conf.MongoDB.Operation]; !ok {
		return nil, fmt.Errorf("mongodb operation '%s' unknown: must be insert-one, delete-one, delete-many, replace-one, update-one or find-one", conf.MongoDB.Operation)
	}

	documentNeeded = documentMapOps[conf.MongoDB.Operation]
	filterNeeded = filterMapOps[conf.MongoDB.Operation]
	hintAllowed = hintAllowedOps[conf.MongoDB.Operation]

	var err error

	if filterNeeded {
		if conf.MongoDB.FilterMap == "" {
			return nil, errors.New("mongodb filter_map must be specified")
		}

		m.filterMap, err = bloblang.NewMapping(conf.MongoDB.FilterMap)
		if err != nil {
			return nil, fmt.Errorf("failed to parse filter_map: %v", err)
		}
	} else if conf.MongoDB.FilterMap != "" {
		return nil, fmt.Errorf("mongodb filter_map not allowed for '%s' operation", conf.MongoDB.Operation)
	}

	if documentNeeded {
		if conf.MongoDB.DocumentMap == "" {
			return nil, errors.New("mongodb document_map must be specified")
		}

		m.documentMap, err = bloblang.NewMapping(conf.MongoDB.DocumentMap)
		if err != nil {
			return nil, fmt.Errorf("failed to parse document_map: %v", err)
		}
	} else if conf.MongoDB.DocumentMap != "" {
		return nil, fmt.Errorf("mongodb document_map not allowed for '%s' operation", conf.MongoDB.Operation)
	}

	if hintAllowed && conf.MongoDB.HintMap != "" {
		m.hintMap, err = bloblang.NewMapping(conf.MongoDB.HintMap)
		if err != nil {
			return nil, fmt.Errorf("failed to parse hint_map: %v", err)
		}
	} else if conf.MongoDB.HintMap != "" {
		return nil, fmt.Errorf("mongodb hint_map not allowed for '%s' operation", conf.MongoDB.Operation)
	}

	m.client, err = conf.MongoDB.MongoDB.Client()
	if err != nil {
		return nil, fmt.Errorf("failed to create mongodb client: %v", err)
	}

	var timeout time.Duration
	if timeout, err = time.ParseDuration(conf.MongoDB.WriteConcern.WTimeout); err != nil {
		return nil, fmt.Errorf("failed to parse wtimeout string: %v", err)
	}
	writeConcern := writeconcern.New(
		writeconcern.J(conf.MongoDB.WriteConcern.J),
		writeconcern.WTimeout(timeout))

	w, err := strconv.Atoi(conf.MongoDB.WriteConcern.W)
	if err != nil {
		writeconcern.WTagSet(conf.MongoDB.WriteConcern.W)
	} else {
		writeconcern.W(w)(writeConcern)
	}

	// This does some validation so we don't have to
	if _, _, err = writeConcern.MarshalBSONValue(); err != nil {
		return nil, fmt.Errorf("write_concern validation error: %w", err)
	}

	m.collection = m.client.
		Database(conf.MongoDB.MongoDB.Database).
		Collection(conf.MongoDB.MongoDB.Collection, options.Collection().SetWriteConcern(writeConcern))

	if m.backoffCtor, err = conf.MongoDB.RetryConfig.GetCtor(); err != nil {
		return nil, err
	}

	m.boffPool = sync.Pool{
		New: func() interface{} {
			return m.backoffCtor()
		},
	}

	closedChan := make(chan struct{})
	m.closedChan = closedChan
	close(closedChan) // starts closed

	err = m.connect()
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (m *MongoDB) connect() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		return errors.New("client is nil") // shouldn't happen
	}

	if m.collection == nil {
		return errors.New("collection is nil") // shouldn't happen
	}

	if m.closingCh != nil {
		select {
		case <-m.closingCh:
			return types.ErrTypeClosed
		default:
		}
	}

	if m.runningCh != nil {
		select {
		default:
			return nil // already connected
		case <-m.runningCh:
		}
	}

	m.runningCh = make(chan struct{})
	m.closingCh = make(chan struct{})

	ctxTimeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := m.client.Connect(ctxTimeout)
	if err != nil {
		close(m.runningCh)
		return fmt.Errorf("failed to connect: %v", err)
	}

	go func() {
		<-m.closingCh

		m.mu.Lock()
		defer m.mu.Unlock()

		defer close(m.runningCh)

		if err := m.client.Disconnect(context.Background()); err != nil {
			m.log.Warnf("error disconnecting: %v\n", err)
		}
	}()

	ctxTimeout, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = m.client.Ping(ctxTimeout, nil)
	if err != nil {
		m.close() // clean up
		return fmt.Errorf("ping failed: %v", err)
	}

	return nil
}

func (m *MongoDB) close() {
	m.closeOnce.Do(func() {
		close(m.closingCh)
	})
}

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (m *MongoDB) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	m.mCount.Incr(1)
	newMsg := msg.Copy()

	boff := m.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		m.boffPool.Put(boff)
	}()

	var writeModels []mongo.WriteModel

	iterErr := newMsg.Iter(func(i int, part types.Part) error {
		var err error
		var filterVal, documentVal types.Part
		var filterValWanted, documentValWanted bool

		filterValWanted = filterMapOps[m.conf.MongoDB.Operation]
		documentValWanted = documentMapOps[m.conf.MongoDB.Operation]

		if filterValWanted {
			filterVal, err = m.filterMap.MapPart(i, msg)
			if err != nil {
				return fmt.Errorf("failed to execute filter_map: %v", err)
			}
		}

		// deleted()
		if (filterVal != nil || !filterValWanted) && documentValWanted {
			documentVal, err = m.documentMap.MapPart(i, msg)
			if err != nil {
				return fmt.Errorf("failed to execute document_map: %v", err)
			}
		}

		if filterVal == nil && filterValWanted {
			return fmt.Errorf("failed to generate filterVal")
		}

		if documentVal == nil && documentValWanted {
			return fmt.Errorf("failed to generate documentVal")
		}

		var docJSON, filterJSON, hintJSON interface{}

		if filterValWanted {
			filterJSON, err = filterVal.JSON()
			if err != nil {
				return err
			}
		}

		if documentValWanted {
			docJSON, err = documentVal.JSON()
			if err != nil {
				return err
			}
		}

		findOptions := &options.FindOneOptions{}
		if m.hintMap != nil {
			hintVal, err := m.hintMap.MapPart(i, msg)
			if err != nil {
				return fmt.Errorf("failed to execute hint_map: %v", err)
			}

			hintJSON, err = hintVal.JSON()
			if err != nil {
				return err
			}
			findOptions.Hint = hintJSON
		}

		var writeModel mongo.WriteModel
		upsertFalse := false
		switch m.conf.MongoDB.Operation {
		case "insert-one":
			writeModel = &mongo.InsertOneModel{
				Document: docJSON,
			}
		case "delete-one":
			writeModel = &mongo.DeleteOneModel{
				Filter: filterJSON,
				Hint:   hintJSON,
			}
		case "delete-many":
			writeModel = &mongo.DeleteManyModel{
				Filter: filterJSON,
				Hint:   hintJSON,
			}
		case "replace-one":
			writeModel = &mongo.ReplaceOneModel{
				Upsert:      &upsertFalse,
				Filter:      filterJSON,
				Replacement: docJSON,
				Hint:        hintJSON,
			}
		case "update-one":
			writeModel = &mongo.UpdateOneModel{
				Upsert: &upsertFalse,
				Filter: filterJSON,
				Update: docJSON,
				Hint:   hintJSON,
			}
		case "find-one":
			result := m.collection.FindOne(context.Background(), filterJSON, findOptions)
			bsonValue, err := result.DecodeBytes()

			if err != nil {
				m.log.Errorf("Error finding document in mongo db, filter = %v", filterJSON)
				return err
			}

			data := map[string]interface{}{}
			elements, err := bsonValue.Elements()

			if err != nil {
				m.log.Errorf("Error getting elements from document in mongo db, filter = %v", filterJSON)
				return err
			}

			for _, e := range elements {
				key := e.Key()
				value := e.Value().String()
				data[key] = value
			}

			err = part.SetJSON(data)
			if err != nil {
				return err
			} else {
				return nil
			}
		}

		if writeModel == nil {
			return nil
		}

		writeModels = append(writeModels, writeModel)

		return nil
	})

	if iterErr != nil {
		m.log.Errorf("error iterating through message parts: %v", iterErr)
	}

	if len(writeModels) > 0 {
		_, err := m.collection.BulkWrite(context.Background(), writeModels)
		if err != nil {
			m.log.Errorf("bulk write failed in mongo processor: %v", err)
		}
	}

	m.mBatchSent.Incr(1)
	m.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *MongoDB) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (c *MongoDB) WaitForClose(_ time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
