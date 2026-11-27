package objectstore

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/ingestion"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
)

func TestManifestReaderBoundsReadsAndDistinguishesMissingObjects(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		body      string
		maxBytes  int64
		permanent bool
	}{
		{name: "success", status: http.StatusOK, body: "{}", maxBytes: 2},
		{name: "excess size", status: http.StatusOK, body: "[1]", maxBytes: 2, permanent: true},
		{name: "missing key", status: http.StatusNotFound, body: "<Error><Code>NoSuchKey</Code><RequestId>storage</RequestId></Error>", maxBytes: 2, permanent: true},
		{name: "missing bucket", status: http.StatusNotFound, body: "<Error><Code>NoSuchBucket</Code><RequestId>storage</RequestId></Error>", maxBytes: 2},
		{name: "ambiguous 404", status: http.StatusNotFound, maxBytes: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("manifest request = %s, want GET", r.Method)
				}
				w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
				w.Header().Set("ETag", `"test"`)
				w.Header().Set("Content-Length", fmt.Sprint(len(test.body)))
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			reader, err := NewManifestReader(config.ObjectStore{
				Endpoint: server.URL, Bucket: "graphx-test", Region: "local", AccessKey: "test", SecretKey: "test",
				PathStyle: true, ConnectTimeout: time.Second, ReadTimeout: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			data, err := reader.Read(t.Context(), "artifact", test.maxBytes)
			if test.status == http.StatusOK && !test.permanent {
				if err != nil || string(data) != test.body {
					t.Fatalf("Read() = %q, %v", data, err)
				}
				return
			}
			if err == nil || ingestion.IsPermanentResultError(err) != test.permanent {
				t.Fatalf("Read() = %q, %v, want permanent=%t", data, err, test.permanent)
			}
		})
	}
}
