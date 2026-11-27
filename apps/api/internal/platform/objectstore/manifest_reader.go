package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/opazdunTeam/GraphX/apps/api/internal/ingestion"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
)

type ManifestReader struct {
	client      *minio.Client
	transport   *http.Transport
	bucket      string
	readTimeout time.Duration
}

func NewManifestReader(cfg config.ObjectStore) (*ManifestReader, error) {
	endpoint, err := url.Parse(cfg.Endpoint)
	if err != nil || endpoint.Host == "" ||
		(endpoint.Scheme != "http" && endpoint.Scheme != "https") ||
		endpoint.User != nil || (endpoint.Path != "" && endpoint.Path != "/") ||
		endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, errors.New("invalid object storage endpoint")
	}
	if strings.TrimSpace(cfg.Bucket) == "" ||
		strings.TrimSpace(cfg.AccessKey) == "" ||
		strings.TrimSpace(cfg.SecretKey) == "" ||
		strings.TrimSpace(cfg.Region) == "" {
		return nil, errors.New("object storage bucket, credentials and region are required")
	}
	if cfg.ConnectTimeout <= 0 || cfg.ReadTimeout <= 0 {
		return nil, errors.New("object storage timeouts must be positive")
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   cfg.ConnectTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   cfg.ConnectTimeout,
		ResponseHeaderTimeout: cfg.ReadTimeout,
		IdleConnTimeout:       90 * time.Second,
	}
	bucketLookup := minio.BucketLookupAuto
	if cfg.PathStyle {
		bucketLookup = minio.BucketLookupPath
	}
	client, err := minio.New(endpoint.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:       endpoint.Scheme == "https",
		Region:       cfg.Region,
		BucketLookup: bucketLookup,
		Transport:    transport,
		MaxRetries:   2,
	})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("initialize object storage client: %w", err)
	}
	return &ManifestReader{
		client:      client,
		transport:   transport,
		bucket:      cfg.Bucket,
		readTimeout: cfg.ReadTimeout,
	}, nil
}

func (r *ManifestReader) Read(
	ctx context.Context,
	objectKey string,
	maxBytes int64,
) ([]byte, error) {
	if strings.TrimSpace(objectKey) == "" || maxBytes < 1 || maxBytes == math.MaxInt64 {
		return nil, errors.New("object key and positive size limit are required")
	}
	readCtx, cancel := context.WithTimeout(ctx, r.readTimeout)
	defer cancel()

	object, err := r.client.GetObject(readCtx, r.bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("open result manifest object: %w", err)
	}
	defer object.Close()

	data, err := io.ReadAll(io.LimitReader(object, maxBytes+1))
	if err != nil {
		if isMissingObject(err) {
			return nil, ingestion.NewPermanentResultErrorWithCode(
				"source_result_manifest_missing", errors.New("result manifest object does not exist"),
			)
		}
		return nil, fmt.Errorf("read result manifest object: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return nil, ingestion.NewPermanentResultError(errors.New("result manifest exceeds configured size limit"))
	}
	return data, nil
}

func (r *ManifestReader) Close() {
	r.transport.CloseIdleConnections()
}

func isMissingObject(err error) bool {
	response := minio.ToErrorResponse(err)
	return response.StatusCode == http.StatusNotFound && response.Code == "NoSuchKey" && response.RequestID != ""
}
