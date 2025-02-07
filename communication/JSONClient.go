package communication

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type Client struct {
	client *http.Client
	logger *log.Logger
}

func NewClient() *Client {
	return &Client{
		client: &http.Client{},
		logger: log.New(os.Stdout, "[orchestrator | communication] ", log.LstdFlags),
	}
}

func (c *Client) Post(ctx context.Context, url, contentType string, data io.Reader) (*http.Response, error) {
	c.logger.Println("Sending \"Post\" request...")
	return c.client.Post(url, contentType, data)
}

func (c *Client) Get(ctx context.Context, url string) (*http.Response, error) {
	c.logger.Println("Sending \"Get\" request...")
	return c.client.Get(url)
}

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	c.logger.Printf("Sending request...")
	return c.client.Do(req)
}

type Body struct {
	Data []byte
	Type BodyType
}

func NewJSON(data []byte) *Body {
	return &Body{
		Data: data,
		Type: JSON,
	}
}

type BodyType int

const (
	JSON BodyType = iota
)

type RequestType int

const (
	Get RequestType = iota
	Post
	Put
	Delete
	Patch
)

func (r RequestType) String() string {
	switch r {
	case Get:
		return "GET"
	case Post:
		return "POST"
	case Put:
		return "PUT"
	case Delete:
		return "DELETE"
	case Patch:
		return "PATCH"
	default:
		log.Fatalf("Invalid type \"%v\"", r)
	}
	return ""
}

func NewPost(ctx context.Context, url string, body *Body) *http.Request {
	buffer := bytes.NewBuffer([]byte{})
	if body != nil {
		buffer = bytes.NewBuffer(body.Data)
	}
	req, err := http.NewRequestWithContext(ctx, Post.String(), url, buffer)
	if err != nil {
		log.Fatalf("Error of creating request: %v", err)
	}
	if body != nil {
		switch body.Type {
		case JSON:
			req.Header.Set("Content-Type", "application/json")
		default:
			log.Fatalf("Unknown body type \"%v\"", body.Type)
		}
	}

	traced := addTraceInfo(ctx, req)
	return traced
}

func NewGet(ctx context.Context, url string) *http.Request {
	req, err := http.NewRequestWithContext(ctx, Get.String(), url, nil)
	if err != nil {
		log.Fatalf("Error of creating request: %v", err)
	}
	traced := addTraceInfo(ctx, req)
	return traced
}

func addTraceInfo(ctx context.Context, request *http.Request) *http.Request {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(request.Header))
	return request
}
