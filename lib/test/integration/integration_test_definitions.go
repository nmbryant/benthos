package integration

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func integrationTestOpenClose() testDefinition {
	return namedTest(
		"can open and close",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			require.NoError(t, sendMessage(env.ctx, t, tranChan, "hello world"))
			messageMatch(t, receiveMessage(env.ctx, t, input.TransactionChan(), nil), "hello world")
		},
	)
}

// The input is created after the output has written data.
func integrationTestOpenCloseIsolated() testDefinition {
	return namedTest(
		"can open and close isolated",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			output := initOutput(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})
			require.NoError(t, sendMessage(env.ctx, t, tranChan, "hello world"))

			input := initInput(t, env)
			t.Cleanup(func() {
				closeConnectors(t, input, nil)
			})
			messageMatch(t, receiveMessage(env.ctx, t, input.TransactionChan(), nil), "hello world")
		},
	)
}

func integrationTestMetadata() testDefinition {
	return namedTest(
		"can send and receive metadata",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			require.NoError(t, sendMessage(
				env.ctx, t, tranChan,
				"hello world",
				"foo", "foo_value",
				"bar", "bar_value",
			))
			messageMatch(
				t, receiveMessage(env.ctx, t, input.TransactionChan(), nil),
				"hello world",
				"foo", "foo_value",
				"bar", "bar_value",
			)
		},
	)
}

func integrationTestSendBatch(n int) testDefinition {
	return namedTest(
		"can send a message batch",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			set := map[string][]string{}
			payloads := []string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world %v", i)
				set[payload] = nil
				payloads = append(payloads, payload)
			}
			sendBatch(env.ctx, t, tranChan, payloads)

			for len(set) > 0 {
				messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
			}
		},
	)
}

func integrationTestSendBatches(batchSize, batches, parallelism int) testDefinition {
	return namedTest(
		"can send many message batches",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			require.Greater(t, parallelism, 0)

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			set := map[string][]string{}
			for j := 0; j < batches; j++ {
				for i := 0; i < batchSize; i++ {
					payload := fmt.Sprintf("hello world %v", j*batches+i)
					set[payload] = nil
				}
			}

			batchChan := make(chan []string)

			var wg sync.WaitGroup
			for k := 0; k < parallelism; k++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						batch, open := <-batchChan
						if !open {
							return
						}
						assert.NoError(t, sendBatch(env.ctx, t, tranChan, batch))
					}
				}()
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				for len(set) > 0 {
					messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
				}
			}()

			for j := 0; j < batches; j++ {
				payloads := []string{}
				for i := 0; i < batchSize; i++ {
					payload := fmt.Sprintf("hello world %v", j*batches+i)
					payloads = append(payloads, payload)
				}
				batchChan <- payloads
			}
			close(batchChan)

			wg.Wait()
		},
	)
}

func integrationTestSendBatchCount(n int) testDefinition {
	return namedTest(
		"can send messages with an output batch count",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			env.configVars.outputBatchCount = n

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			resChan := make(chan types.Response)

			set := map[string][]string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world %v", i)
				set[payload] = nil
				msg := message.New(nil)
				msg.Append(message.NewPart([]byte(payload)))
				select {
				case tranChan <- types.NewTransaction(msg, resChan):
				case res := <-resChan:
					t.Fatalf("premature response: %v", res.Error())
				case <-env.ctx.Done():
					t.Fatal("timed out on send")
				}
			}

			for i := 0; i < n; i++ {
				select {
				case res := <-resChan:
					assert.NoError(t, res.Error())
				case <-env.ctx.Done():
					t.Fatal("timed out on response")
				}
			}

			for len(set) > 0 {
				messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
			}
		},
	)
}

// The input is created after the output has written data.
func integrationTestSendBatchCountIsolated(n int) testDefinition {
	return namedTest(
		"can send messages with an output batch count isolated",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			env.configVars.outputBatchCount = n

			tranChan := make(chan types.Transaction)
			output := initOutput(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			resChan := make(chan types.Response)

			set := map[string][]string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world %v", i)
				set[payload] = nil
				msg := message.New(nil)
				msg.Append(message.NewPart([]byte(payload)))
				select {
				case tranChan <- types.NewTransaction(msg, resChan):
				case res := <-resChan:
					t.Fatalf("premature response: %v", res.Error())
				case <-env.ctx.Done():
					t.Fatal("timed out on send")
				}
			}

			for i := 0; i < n; i++ {
				select {
				case res := <-resChan:
					assert.NoError(t, res.Error())
				case <-env.ctx.Done():
					t.Fatal("timed out on response")
				}
			}

			input := initInput(t, env)
			t.Cleanup(func() {
				closeConnectors(t, input, nil)
			})

			for len(set) > 0 {
				messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
			}
		},
	)
}

func integrationTestReceiveBatchCount(n int) testDefinition {
	return namedTest(
		"can send messages with an input batch count",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			env.configVars.inputBatchCount = n

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			set := map[string][]string{}

			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
				require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
			}

			var tran types.Transaction
			select {
			case tran = <-input.TransactionChan():
			case <-env.ctx.Done():
				t.Fatal("timed out on receive")
			}

			assert.Equal(t, n, tran.Payload.Len())
			tran.Payload.Iter(func(_ int, p types.Part) error {
				messageInSet(t, true, false, p, set)
				return nil
			})

			select {
			case tran.ResponseChan <- response.NewAck():
			case <-env.ctx.Done():
				t.Fatal("timed out on response")
			}
		},
	)
}

func integrationTestStreamSequential(n int) testDefinition {
	return namedTest(
		"can send and receive data sequentially",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			set := map[string][]string{}

			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
				require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
			}

			for len(set) > 0 {
				messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
			}
		},
	)
}

// The input is created after the output has written data.
func integrationTestStreamIsolated(n int) testDefinition {
	return namedTest(
		"can send and receive data isolated",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			output := initOutput(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			set := map[string][]string{}

			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
				require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
			}

			input := initInput(t, env)
			t.Cleanup(func() {
				closeConnectors(t, input, nil)
			})

			for len(set) > 0 {
				messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
			}
		},
	)
}

func integrationTestStreamParallel(n int) testDefinition {
	return namedTest(
		"can send and receive data in parallel",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction, n)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			set := map[string][]string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
			}

			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					payload := fmt.Sprintf("hello world: %v", i)
					require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
				}
			}()

			go func() {
				defer wg.Done()
				for len(set) > 0 {
					messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
				}
			}()

			wg.Wait()
		},
	)
}

func integrationTestStreamParallelLossy(n int) testDefinition {
	return namedTest(
		"can send and receive data in parallel lossy",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			set := map[string][]string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
			}

			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					payload := fmt.Sprintf("hello world: %v", i)
					require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
				}
			}()

			go func() {
				defer wg.Done()
				rejected := 0
				for i := 0; i < n; i++ {
					if i%10 == 1 {
						rejected++
						messageInSet(
							t, false, true,
							receiveMessage(env.ctx, t, input.TransactionChan(), errors.New("rejected just cus")),
							set,
						)
					} else {
						messageInSet(t, true, true, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
					}
				}

				t.Log("Finished first loop, looping through rejected messages.")
				for len(set) > 0 {
					messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
				}
			}()

			wg.Wait()
		},
	)
}

func integrationTestStreamParallelLossyThroughReconnect(n int) testDefinition {
	return namedTest(
		"can send and receive data in parallel lossy through reconnect",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			set := map[string][]string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
			}

			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					payload := fmt.Sprintf("hello world: %v", i)
					require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
				}
			}()

			go func() {
				defer wg.Done()
				rejected := 0
				for i := 0; i < n; i++ {
					if i%10 == 1 {
						rejected++
						messageInSet(
							t, false, env.allowDuplicateMessages,
							receiveMessage(env.ctx, t, input.TransactionChan(), errors.New("rejected just cus")),
							set,
						)
					} else {
						messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
					}
				}

				closeConnectors(t, input, nil)

				input = initInput(t, env)
				t.Cleanup(func() {
					closeConnectors(t, input, nil)
				})

				t.Log("Finished first loop, looping through rejected messages.")
				for len(set) > 0 {
					messageInSet(t, true, true, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
				}
			}()

			wg.Wait()
		},
	)
}

// With a given identifier, extract the message from the target output
// destination. This is normally used for testing cache or DB based outputs that
// don't have a stream consumer available.
type getMessageFunc func(*testEnvironment, string) (string, []string, error)

func integrationTestOutputOnlySendSequential(n int, getFn getMessageFunc) testDefinition {
	return namedTest(
		"can send to output",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			output := initOutput(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			set := map[string]string{}
			for i := 0; i < n; i++ {
				id := strconv.Itoa(i)
				payload := fmt.Sprintf(`{"id":%v,"content":"hello world"}`, id)
				set[id] = payload
				require.NoError(t, sendMessage(env.ctx, t, tranChan, payload, "id", id))
			}

			for k, exp := range set {
				act, _, err := getFn(env, k)
				require.NoError(t, err)
				assert.Equal(t, exp, act)
			}
		},
	)
}

func integrationTestOutputOnlySendBatch(n int, getFn getMessageFunc) testDefinition {
	return namedTest(
		"can send to output as batch",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			output := initOutput(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			set := map[string]string{}
			batch := []string{}
			for i := 0; i < n; i++ {
				id := strconv.Itoa(i)
				payload := fmt.Sprintf(`{"id":%v,"content":"hello world"}`, id)
				set[id] = payload
				batch = append(batch, payload)
			}
			require.NoError(t, sendBatch(env.ctx, t, tranChan, batch))

			for k, exp := range set {
				act, _, err := getFn(env, k)
				require.NoError(t, err)
				assert.Equal(t, exp, act)
			}
		},
	)
}

func integrationTestOutputOnlyOverride(getFn getMessageFunc) testDefinition {
	return namedTest(
		"can send to output and override value",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			output := initOutput(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			first := `{"id":1,"content":"this should be overridden"}`
			exp := `{"id":1,"content":"hello world"}`
			require.NoError(t, sendMessage(env.ctx, t, tranChan, first))
			require.NoError(t, sendMessage(env.ctx, t, tranChan, exp))

			act, _, err := getFn(env, "1")
			require.NoError(t, err)
			assert.Equal(t, exp, act)
		},
	)
}
