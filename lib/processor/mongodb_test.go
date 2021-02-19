// +build integration

package processor

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	bmongo "github.com/Jeffail/benthos/v3/internal/service/mongodb"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestMongoDBProcessor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}

	portBindings := map[docker.Port][]docker.PortBinding{}
	portBindings["27017/tcp"] = []docker.PortBinding{{
		HostIP:   "",
		HostPort: "27017/tcp",
	}}

	env := []string{
		"MONGO_INITDB_ROOT_USERNAME=mongoadmin",
		"MONGO_INITDB_ROOT_PASSWORD=secret",
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "mongo",
		Tag:          "latest",
		Env:          env,
		PortBindings: portBindings,
	})
	require.NoError(t, err)

	time.Sleep(time.Second * 1)

	_, err = resource.Exec([]string{
		"mongo",
		"-u",
		"mongoadmin",
		"-p",
		"secret",
		"--authenticationDatabase",
		"admin",
		"TestDB",
	}, dockertest.ExecOptions{})
	require.NoError(t, err)

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	var mongoClient *mongo.Client
	require.NoError(t, pool.Retry(func() error {
		url := "mongodb://localhost:27017"
		conf := bmongo.NewConfig()
		conf.URL = url
		conf.Database = "TestDB"
		conf.Collection = "TestCollection"
		conf.Username = "mongoadmin"
		conf.Password = "secret"

		mongoClient, err = conf.Client()
		if err != nil {
			return err
		}

		err = mongoClient.Connect(context.Background())
		if err != nil {
			return err
		}

		createCollection(resource, "TestCollection", "mongoadmin", "secret")

		return nil
	}))

	t.Run("testMongoProcessor", func(t *testing.T) {
		testMongoDBProcessorInsert(t)
		testMongoDBProcessorDeleteOne(t)
		testMongoDBProcessorDeleteMany(t)
		testMongoDBProcessorReplaceOne(t)
		testMongoDBProcessorUpdateOne(t)
		testMongoDBProcessorFindOne(t)
	})
}

func testMongoDBProcessorInsert(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeMongoDB

	c := bmongo.Config{
		URL:        "mongodb://localhost:27017",
		Database:   "TestDB",
		Collection: "TestCollection",
		Username:   "mongoadmin",
		Password:   "secret",
	}

	mongoConfig := MongoDBConfig{
		MongoDB: c,
		WriteConcern: bmongo.WriteConcern{
			W:        "1",
			J:        false,
			WTimeout: "100s",
		},
		Parts:       nil,
		Operation:   "insert-one",
		DocumentMap: "root.a = this.foo\nroot.b = this.bar",
	}

	conf.MongoDB = mongoConfig

	m, err := NewMongoDB(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo1","bar":"bar1"}`),
		[]byte(`{"foo":"foo2","bar":"bar2"}`),
	}

	resMsgs, response := m.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expectedResult := [][]byte{
		[]byte(`{"foo":"foo1","bar":"bar1"}`),
		[]byte(`{"foo":"foo2","bar":"bar2"}`),
	}

	assert.Equal(t, expectedResult, message.GetAllBytes(resMsgs[0]))

	// Validate the record is in the MongoDB
	mongoClient, err := c.Client()
	require.NoError(t, err)
	err = mongoClient.Connect(context.Background())
	require.NoError(t, err)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")

	result := collection.FindOne(context.Background(), bson.M{"a": "foo1", "b": "bar1"})
	b, err := result.DecodeBytes()
	assert.NoError(t, err)
	aVal := b.Lookup("a")
	bVal := b.Lookup("b")
	assert.Equal(t, `"foo1"`, aVal.String())
	assert.Equal(t, `"bar1"`, bVal.String())

	result = collection.FindOne(context.Background(), bson.M{"a": "foo2", "b": "bar2"})
	b, err = result.DecodeBytes()
	assert.NoError(t, err)
	aVal = b.Lookup("a")
	bVal = b.Lookup("b")
	assert.Equal(t, `"foo2"`, aVal.String())
	assert.Equal(t, `"bar2"`, bVal.String())
}

func testMongoDBProcessorDeleteOne(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeMongoDB

	c := bmongo.Config{
		URL:        "mongodb://localhost:27017",
		Database:   "TestDB",
		Collection: "TestCollection",
		Username:   "mongoadmin",
		Password:   "secret",
	}

	mongoConfig := MongoDBConfig{
		MongoDB: c,
		WriteConcern: bmongo.WriteConcern{
			W:        "1",
			J:        false,
			WTimeout: "100s",
		},
		Parts:     nil,
		Operation: "delete-one",
		FilterMap: "root.a = this.foo\nroot.b = this.bar",
	}

	mongoClient, err := c.Client()
	require.NoError(t, err)
	err = mongoClient.Connect(context.Background())
	require.NoError(t, err)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")
	_, err = collection.InsertOne(context.Background(), bson.M{"a": "foo_delete", "b": "bar_delete"})
	assert.NoError(t, err)

	conf.MongoDB = mongoConfig
	m, err := NewMongoDB(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo_delete","bar":"bar_delete"}`),
	}

	resMsgs, response := m.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expectedResult := [][]byte{
		[]byte(`{"foo":"foo_delete","bar":"bar_delete"}`),
	}

	assert.Equal(t, expectedResult, message.GetAllBytes(resMsgs[0]))

	// Validate the record has been deleted from the db
	result := collection.FindOne(context.Background(), bson.M{"a": "foo_delete", "b": "bar_delete"})
	b, err := result.DecodeBytes()
	assert.Nil(t, b)
	assert.Error(t, err, "mongo: no documents in result")
}

func testMongoDBProcessorDeleteMany(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeMongoDB

	c := bmongo.Config{
		URL:        "mongodb://localhost:27017",
		Database:   "TestDB",
		Collection: "TestCollection",
		Username:   "mongoadmin",
		Password:   "secret",
	}

	mongoConfig := MongoDBConfig{
		MongoDB: c,
		WriteConcern: bmongo.WriteConcern{
			W:        "1",
			J:        false,
			WTimeout: "100s",
		},
		Parts:     nil,
		Operation: "delete-many",
		FilterMap: "root.a = this.foo\nroot.b = this.bar",
	}

	mongoClient, err := c.Client()
	require.NoError(t, err)
	err = mongoClient.Connect(context.Background())
	require.NoError(t, err)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")
	_, err = collection.InsertOne(context.Background(), bson.M{"a": "foo_delete_many", "b": "bar_delete_many", "c": "c1"})
	assert.NoError(t, err)
	_, err = collection.InsertOne(context.Background(), bson.M{"a": "foo_delete_many", "b": "bar_delete_many", "c": "c2"})
	assert.NoError(t, err)

	conf.MongoDB = mongoConfig
	m, err := NewMongoDB(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo_delete_many","bar":"bar_delete_many"}`),
	}

	resMsgs, response := m.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expectedResult := [][]byte{
		[]byte(`{"foo":"foo_delete_many","bar":"bar_delete_many"}`),
	}
	assert.Equal(t, expectedResult, message.GetAllBytes(resMsgs[0]))

	// Validate the record has been deleted from the db
	result := collection.FindOne(context.Background(), bson.M{"a": "foo_delete_many", "b": "bar_delete_many"})
	b, err := result.DecodeBytes()
	assert.Nil(t, b)
	assert.Error(t, err, "mongo: no documents in result")
}

func testMongoDBProcessorReplaceOne(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeMongoDB

	c := bmongo.Config{
		URL:        "mongodb://localhost:27017",
		Database:   "TestDB",
		Collection: "TestCollection",
		Username:   "mongoadmin",
		Password:   "secret",
	}

	mongoConfig := MongoDBConfig{
		MongoDB: c,
		WriteConcern: bmongo.WriteConcern{
			W:        "1",
			J:        false,
			WTimeout: "100s",
		},
		Parts:       nil,
		Operation:   "replace-one",
		DocumentMap: "root.a = this.foo\nroot.b = this.bar",
		FilterMap:   "root.a = this.foo",
	}

	mongoClient, err := c.Client()
	require.NoError(t, err)
	err = mongoClient.Connect(context.Background())
	require.NoError(t, err)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")
	_, err = collection.InsertOne(context.Background(), bson.M{"a": "foo_replace", "b": "bar_old", "c": "c1"})
	assert.NoError(t, err)

	conf.MongoDB = mongoConfig
	m, err := NewMongoDB(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo_replace","bar":"bar_new"}`),
	}

	resMsgs, response := m.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expectedResult := [][]byte{
		[]byte(`{"foo":"foo_replace","bar":"bar_new"}`),
	}
	assert.Equal(t, expectedResult, message.GetAllBytes(resMsgs[0]))

	// Validate the record has been updated in the db
	result := collection.FindOne(context.Background(), bson.M{"a": "foo_replace", "b": "bar_new"})
	b, err := result.DecodeBytes()
	assert.NoError(t, err)
	aVal := b.Lookup("a")
	bVal := b.Lookup("b")
	cVal := b.Lookup("c")
	assert.Equal(t, `"foo_replace"`, aVal.String())
	assert.Equal(t, `"bar_new"`, bVal.String())
	assert.Equal(t, bson.RawValue{}, cVal)
}

func testMongoDBProcessorUpdateOne(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeMongoDB

	c := bmongo.Config{
		URL:        "mongodb://localhost:27017",
		Database:   "TestDB",
		Collection: "TestCollection",
		Username:   "mongoadmin",
		Password:   "secret",
	}

	mongoConfig := MongoDBConfig{
		MongoDB: c,
		WriteConcern: bmongo.WriteConcern{
			W:        "1",
			J:        false,
			WTimeout: "100s",
		},
		Parts:       nil,
		Operation:   "update-one",
		DocumentMap: `root = {"$set": {"a": this.foo, "b": this.bar}}`,
		FilterMap:   "root.a = this.foo",
	}

	mongoClient, err := c.Client()
	require.NoError(t, err)
	err = mongoClient.Connect(context.Background())
	require.NoError(t, err)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")
	_, err = collection.InsertOne(context.Background(), bson.M{"a": "foo_update", "b": "bar_update_old", "c": "c1"})
	assert.NoError(t, err)

	conf.MongoDB = mongoConfig
	m, err := NewMongoDB(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo_update","bar":"bar_update_new"}`),
	}

	resMsgs, response := m.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expectedResult := [][]byte{
		[]byte(`{"foo":"foo_update","bar":"bar_update_new"}`),
	}
	assert.Equal(t, expectedResult, message.GetAllBytes(resMsgs[0]))

	// Validate the record has been updated in the db
	result := collection.FindOne(context.Background(), bson.M{"a": "foo_update", "b": "bar_update_new"})
	b, err := result.DecodeBytes()
	assert.NoError(t, err)
	aVal := b.Lookup("a")
	bVal := b.Lookup("b")
	cVal := b.Lookup("c")
	assert.Equal(t, `"foo_update"`, aVal.String())
	assert.Equal(t, `"bar_update_new"`, bVal.String())
	assert.Equal(t, `"c1"`, cVal.String())
}

func testMongoDBProcessorFindOne(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeMongoDB

	c := bmongo.Config{
		URL:        "mongodb://localhost:27017",
		Database:   "TestDB",
		Collection: "TestCollection",
		Username:   "mongoadmin",
		Password:   "secret",
	}

	mongoConfig := MongoDBConfig{
		MongoDB: c,
		WriteConcern: bmongo.WriteConcern{
			W:        "1",
			J:        false,
			WTimeout: "100s",
		},
		Parts:     nil,
		Operation: "find-one",
		FilterMap: "root.a = this.foo",
	}

	mongoClient, err := c.Client()
	require.NoError(t, err)
	err = mongoClient.Connect(context.Background())
	require.NoError(t, err)
	collection := mongoClient.Database("TestDB").Collection("TestCollection")
	_, err = collection.InsertOne(context.Background(), bson.M{"a": "foo_find", "b": "bar_find", "c": "c1"})
	assert.NoError(t, err)

	conf.MongoDB = mongoConfig
	m, err := NewMongoDB(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo_find","bar":"bar_update_new"}`),
	}

	resMsgs, response := m.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	resStr := string(resMsgs[0].Get(0).Get())
	if resStr != "" {
	}

	expected := (`{"_id":"{\"$oid\":\"*\"}","a":"\"foo_find\"","b":"\"bar_find\"","c":"\"c1\""}`)
	assert.True(t, match(expected, string(resMsgs[0].Get(0).Get())))
}

func createCollection(resource *dockertest.Resource, collectionName string, username string, password string) error {
	time.Sleep(time.Second * 1)

	_, err := resource.Exec([]string{
		"mongo",
		"-u",
		username,
		"-p",
		password,
		"--authenticationDatabase",
		"admin",
		"--eval",
		"db.createCollection(\"" + collectionName + "\")",
		"TestDB",
	}, dockertest.ExecOptions{})
	if err != nil {
		return err
	}

	time.Sleep(time.Second * 1)

	return nil
}

// wildCardToRegexp converts a wildcard pattern to a regular expression pattern.
func wildCardToRegexp(pattern string) string {
	var result strings.Builder
	for i, literal := range strings.Split(pattern, "*") {

		// Replace * with .*
		if i > 0 {
			result.WriteString(".*")
		}

		// Quote any regular expression meta characters in the
		// literal text.
		result.WriteString(regexp.QuoteMeta(literal))
	}
	return result.String()
}

func match(pattern string, value string) bool {
	result, _ := regexp.MatchString(wildCardToRegexp(pattern), value)
	return result
}
