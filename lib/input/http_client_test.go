package input

import (
	"bytes"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

func TestHTTPClientGET(t *testing.T) {
	inputs := []string{
		"foo1",
		"foo2",
		"foo3",
		"foo4",
		"foo5",
	}

	var reqCount uint32
	index := 0

	var reqMut sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqMut.Lock()
		defer reqMut.Unlock()

		if exp, act := "GET", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}
		atomic.AddUint32(&reqCount, 1)
		w.Write([]byte(inputs[index%len(inputs)]))
		index++
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = ts.URL + "/testpost"
	conf.HTTPClient.RetryMS = 1
	conf.HTTPClient.NumRetries = 3

	h, err := NewHTTPClient(conf, nil, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	var tr types.Transaction
	var open bool

	reqMut.Lock()
	for _, expPart := range inputs {
		reqMut.Unlock()
		select {
		case tr, open = <-h.TransactionChan():
			if !open {
				t.Fatal("Chan not open")
			}
			if exp, act := 1, tr.Payload.Len(); exp != act {
				t.Fatalf("Wrong count of parts: %v != %v", act, exp)
			}
			if exp, act := expPart, string(tr.Payload.Get(0)); exp != act {
				t.Errorf("Wrong part: %v != %v", act, exp)
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}

		reqMut.Lock()
		select {
		case tr.ResponseChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}
	}

	h.CloseAsync()
	reqMut.Unlock()
	select {
	case <-h.TransactionChan():
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
	}

	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if exp, act := uint32(len(inputs)), atomic.LoadUint32(&reqCount); exp != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", act, exp)
	}
}

func TestHTTPClientPOST(t *testing.T) {
	var reqCount uint32
	inputs := []string{
		"foo1",
		"foo2",
		"foo3",
		"foo4",
		"foo5",
	}

	var reqMut sync.Mutex

	index := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqMut.Lock()
		defer reqMut.Unlock()

		if exp, act := "POST", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}
		defer r.Body.Close()

		bodyBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}

		if exp, act := "foobar", string(bodyBytes); exp != act {
			t.Errorf("Wrong post body: %v != %v", act, exp)
		}

		atomic.AddUint32(&reqCount, 1)
		w.Write([]byte(inputs[index%len(inputs)]))
		index++
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = ts.URL + "/testpost"
	conf.HTTPClient.Verb = "POST"
	conf.HTTPClient.Payload = "foobar"
	conf.HTTPClient.RetryMS = 1
	conf.HTTPClient.NumRetries = 3

	h, err := NewHTTPClient(conf, nil, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	reqMut.Lock()
	for _, expPart := range inputs {
		var ts types.Transaction
		var open bool

		reqMut.Unlock()
		select {
		case ts, open = <-h.TransactionChan():
			if !open {
				t.Fatal("Chan not open")
			}
			if exp, act := 1, ts.Payload.Len(); exp != act {
				t.Fatalf("Wrong count of parts: %v != %v", act, exp)
			}
			if exp, act := expPart, string(ts.Payload.Get(0)); exp != act {
				t.Errorf("Wrong part: %v != %v", act, exp)
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}

		reqMut.Lock()
		select {
		case ts.ResponseChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}
	}

	h.CloseAsync()
	reqMut.Unlock()

	select {
	case <-h.TransactionChan():
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
	}

	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if exp, act := uint32(len(inputs)), atomic.LoadUint32(&reqCount); exp != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", act, exp)
	}
}

func TestHTTPClientGETMultipart(t *testing.T) {
	var reqMut sync.Mutex

	var reqCount uint32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqMut.Lock()
		defer reqMut.Unlock()

		if exp, act := "GET", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}
		atomic.AddUint32(&reqCount, 1)

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		parts := []string{
			"hello", "http", "world",
		}
		for _, p := range parts {
			var err error
			var part io.Writer
			if part, err = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{"application/octet-stream"},
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader([]byte(p)))
			}
			if err != nil {
				t.Fatal(err)
			}
		}

		writer.Close()
		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.Write(body.Bytes())
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = ts.URL + "/testpost"
	conf.HTTPClient.RetryMS = 1
	conf.HTTPClient.NumRetries = 3

	h, err := NewHTTPClient(conf, nil, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	var tr types.Transaction
	var open bool

	select {
	case tr, open = <-h.TransactionChan():
		if !open {
			t.Fatal("Chan not open")
		}
		if exp, act := 3, tr.Payload.Len(); exp != act {
			t.Fatalf("Wrong count of parts: %v != %v", act, exp)
		}
		if exp, act := "hello", string(tr.Payload.Get(0)); exp != act {
			t.Errorf("Wrong part: %v != %v", act, exp)
		}
		if exp, act := "http", string(tr.Payload.Get(1)); exp != act {
			t.Errorf("Wrong part: %v != %v", act, exp)
		}
		if exp, act := "world", string(tr.Payload.Get(2)); exp != act {
			t.Errorf("Wrong part: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
	}

	reqMut.Lock()
	select {
	case tr.ResponseChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
	}
	h.CloseAsync()
	reqMut.Unlock()

	select {
	case <-h.TransactionChan():
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
	}

	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if exp, act := uint32(1), atomic.LoadUint32(&reqCount); exp != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", act, exp)
	}
}

func TestHTTPClientGETMultipartLoop(t *testing.T) {
	tests := [][]string{
		{
			"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
			"Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
			"Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
		},
		{
			"Tristique et egestas quis ipsum suspendisse ultrices. Quis enim lobortis scelerisque fermentum dui faucibus.",
		},
		{
			"Lorem donec massa sapien faucibus et molestie ac. Lectus proin nibh nisl condimentum id venenatis a.",
			"Ultricies mi eget mauris pharetra et ultrices neque ornare aenean.",
		},
		{
			"Amet tellus cras adipiscing enim. Non pulvinar neque laoreet suspendisse interdum consectetur. Venenatis cras sed felis eget velit aliquet sagittis.",
			"Ac feugiat sed lectus vestibulum mattis ullamcorper velit. Phasellus vestibulum lorem sed risus ultricies tristique nulla aliquet.",
			"Odio ut sem nulla pharetra diam sit. Neque vitae tempus quam pellentesque nec nam aliquam sem.",
			"Scelerisque eu ultrices vitae auctor eu augue. Ut eu sem integer vitae justo eget. Purus in massa tempor nec feugiat nisl pretium fusce id.",
		},
	}

	var reqMut sync.Mutex

	var index int
	tserve := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqMut.Lock()
		defer reqMut.Unlock()

		if exp, act := "GET", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		parts := tests[index%len(tests)]
		for _, p := range parts {
			var err error
			var part io.Writer
			if part, err = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{"application/octet-stream"},
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader([]byte(p)))
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		index++

		writer.Close()
		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.Write(body.Bytes())
	}))
	defer tserve.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = tserve.URL + "/testpost"
	conf.HTTPClient.RetryMS = 1
	conf.HTTPClient.NumRetries = 3

	h, err := NewHTTPClient(conf, nil, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	reqMut.Lock()
	for _, test := range tests {
		var ts types.Transaction
		var open bool

		reqMut.Unlock()
		select {
		case ts, open = <-h.TransactionChan():
			if !open {
				t.Fatal("Chan not open")
			}
			if exp, act := len(test), ts.Payload.Len(); exp != act {
				t.Fatalf("Wrong count of parts: %v != %v", act, exp)
			}
			for i, part := range test {
				if exp, act := part, string(ts.Payload.Get(i)); exp != act {
					t.Errorf("Wrong part: %v != %v", act, exp)
				}
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}

		reqMut.Lock()
		select {
		case ts.ResponseChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}
	}

	h.CloseAsync()
	reqMut.Unlock()

	select {
	case <-h.TransactionChan():
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
	}

	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientStreamGETMultipartLoop(t *testing.T) {
	tests := [][]string{
		{
			"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
			"Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
			"Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
		},
		{
			"Tristique et egestas quis ipsum suspendisse ultrices. Quis enim lobortis scelerisque fermentum dui faucibus.",
		},
		{
			"Lorem donec massa sapien faucibus et molestie ac. Lectus proin nibh nisl condimentum id venenatis a.",
			"Ultricies mi eget mauris pharetra et ultrices neque ornare aenean.",
		},
		{
			"Amet tellus cras adipiscing enim. Non pulvinar neque laoreet suspendisse interdum consectetur. Venenatis cras sed felis eget velit aliquet sagittis.",
			"Ac feugiat sed lectus vestibulum mattis ullamcorper velit. Phasellus vestibulum lorem sed risus ultricies tristique nulla aliquet.",
			"Odio ut sem nulla pharetra diam sit. Neque vitae tempus quam pellentesque nec nam aliquam sem.",
			"Scelerisque eu ultrices vitae auctor eu augue. Ut eu sem integer vitae justo eget. Purus in massa tempor nec feugiat nisl pretium fusce id.",
		},
	}

	tserve := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exp, act := "GET", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}

		body := &bytes.Buffer{}

		for _, test := range tests {
			for _, part := range test {
				body.WriteString(part)
				body.WriteByte('\n')
			}
			body.WriteByte('\n')
		}
		body.WriteString("A msg that we won't read\nsecond part\n\n")

		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(body.Bytes())
	}))
	defer tserve.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = tserve.URL + "/testpost"
	conf.HTTPClient.RetryMS = 1
	conf.HTTPClient.NumRetries = 3
	conf.HTTPClient.Stream.Enabled = true
	conf.HTTPClient.Stream.Multipart = true

	h, err := NewHTTPClient(conf, nil, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	for _, test := range tests {
		var ts types.Transaction
		var open bool

		select {
		case ts, open = <-h.TransactionChan():
			if !open {
				t.Fatal("Chan not open")
			}
			if exp, act := len(test), ts.Payload.Len(); exp != act {
				t.Fatalf("Wrong count of parts: %v != %v", act, exp)
			}
			for i, part := range test {
				if exp, act := part, string(ts.Payload.Get(i)); exp != act {
					t.Errorf("Wrong part: %v != %v", act, exp)
				}
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}

		select {
		case ts.ResponseChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientStreamGETMultiRecover(t *testing.T) {
	msgs := [][]string{
		{"foo", "bar"},
		{"foo", "baz"},
	}

	tserve := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exp, act := "GET", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}

		body := &bytes.Buffer{}
		for _, msg := range msgs {
			for _, part := range msg {
				body.WriteString(part)
				body.WriteByte('\n')
			}
			body.WriteByte('\n')
		}

		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(body.Bytes())
	}))
	defer tserve.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = tserve.URL + "/testpost"
	conf.HTTPClient.RetryMS = 1
	conf.HTTPClient.NumRetries = 3
	conf.HTTPClient.Stream.Enabled = true
	conf.HTTPClient.Stream.Multipart = true

	h, err := NewHTTPClient(conf, nil, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		for _, testMsg := range msgs {
			var ts types.Transaction
			var open bool
			select {
			case ts, open = <-h.TransactionChan():
				if !open {
					t.Fatal("Chan not open")
				}
				if exp, act := len(testMsg), ts.Payload.Len(); exp != act {
					t.Fatalf("Wrong count of parts: %v != %v", act, exp)
				}
				for j, part := range testMsg {
					if exp, act := part, string(ts.Payload.Get(j)); exp != act {
						t.Errorf("Wrong part: %v != %v", act, exp)
					}
				}
			case <-time.After(time.Second):
				t.Errorf("Action timed out")
			}

			select {
			case ts.ResponseChan <- types.NewSimpleResponse(nil):
			case <-time.After(time.Second):
				t.Errorf("Action timed out")
			}
		}
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientStreamGETRecover(t *testing.T) {
	msgs := []string{"foo", "bar"}

	tserve := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exp, act := "GET", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}

		body := &bytes.Buffer{}
		for _, msg := range msgs {
			body.WriteString(msg)
			body.WriteByte('\n')
		}

		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(body.Bytes())
	}))
	defer tserve.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = tserve.URL + "/testpost"
	conf.HTTPClient.RetryMS = 1
	conf.HTTPClient.NumRetries = 3
	conf.HTTPClient.Stream.Enabled = true
	conf.HTTPClient.Stream.Multipart = false

	h, err := NewHTTPClient(conf, nil, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		for _, testMsg := range msgs {
			var ts types.Transaction
			var open bool
			select {
			case ts, open = <-h.TransactionChan():
				if !open {
					t.Fatal("Chan not open")
				}
				if exp, act := 1, ts.Payload.Len(); exp != act {
					t.Fatalf("Wrong count of parts: %v != %v", act, exp)
				}
				if exp, act := testMsg, string(ts.Payload.Get(0)); exp != act {
					t.Errorf("Wrong part: %v != %v", act, exp)
				}
			case <-time.After(time.Second):
				t.Errorf("Action timed out")
			}

			select {
			case ts.ResponseChan <- types.NewSimpleResponse(nil):
			case <-time.After(time.Second):
				t.Errorf("Action timed out")
			}
		}
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func BenchmarkHTTPClientGETMultipart(b *testing.B) {
	parts := []string{
		"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
		"Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
		"Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
	}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	for _, p := range parts {
		var err error
		var part io.Writer
		if part, err = writer.CreatePart(textproto.MIMEHeader{
			"Content-Type": []string{"application/octet-stream"},
		}); err == nil {
			_, err = io.Copy(part, bytes.NewReader([]byte(p)))
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	writer.Close()
	header := writer.FormDataContentType()

	tserve := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exp, act := "GET", r.Method; exp != act {
			b.Errorf("Wrong method: %v != %v", act, exp)
		}

		w.Header().Add("Content-Type", header)
		w.Write(body.Bytes())
	}))
	defer tserve.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = tserve.URL + "/testpost"
	conf.HTTPClient.RetryMS = 1
	conf.HTTPClient.NumRetries = 3

	h, err := NewHTTPClient(conf, nil, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		b.Error(err)
		return
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ts, open := <-h.TransactionChan()
		if !open {
			b.Fatal("Chan not open")
		}
		if exp, act := 3, ts.Payload.Len(); exp != act {
			b.Fatalf("Wrong count of parts: %v != %v", act, exp)
		}
		for i, part := range parts {
			if exp, act := part, string(ts.Payload.Get(i)); exp != act {
				b.Errorf("Wrong part: %v != %v", act, exp)
			}
		}
		ts.ResponseChan <- types.NewSimpleResponse(nil)
	}

	b.StopTimer()

	h.CloseAsync()
	if err := h.WaitForClose(time.Second); err != nil {
		b.Error(err)
	}
}