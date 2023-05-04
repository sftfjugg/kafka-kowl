//go:build integration

package console

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/sync/errgroup"

	toxiServer "github.com/Shopify/toxiproxy/v2"
	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/Shopify/toxiproxy/v2/stream"
	"github.com/Shopify/toxiproxy/v2/toxics"
	"github.com/rs/zerolog"
)

func Test_ListMessagesProxied(t *testing.T) {
	fmt.Printf("\n\nListMessagesProxied\n")

	ctx := context.Background()

	trueClient, err := kgo.NewClient(
		kgo.SeedBrokers(testSeedBroker),
	)

	kafkaAdmCl := kadm.NewClient(trueClient)

	defer trueClient.Close()

	_, err = kafkaAdmCl.CreateTopic(ctx, 1, 1, nil, "console_toxiproxy_test")
	assert.NoError(t, err)

	g := new(errgroup.Group)
	g.Go(func() error {
		produceOrders(t, ctx, trueClient, "console_toxiproxy_test")
		return nil
	})

	err = g.Wait()
	assert.NoError(t, err)

	toxics.Register("debug", new(DebugToxic))
	toxics.Register("kafka_upstream_parse", new(KafkaUpstreamToxic))
	toxics.Register("kafka_downstream_parse", new(KafkaDownstreamToxic))

	runToxiproxyServer(t, 8474)

	proxyAddr := "localhost:35432"

	toxi := toxiproxy.NewClient("localhost:8474")
	_, err = toxi.Populate([]toxiproxy.Proxy{{
		Name:     "redpanda",
		Listen:   proxyAddr,
		Upstream: testSeedBroker,
		Enabled:  true,
	}})
	assert.NoError(t, err)

	proxies, err := toxi.Proxies()
	assert.NoError(t, err)

	fmt.Println("SEED:", testSeedBroker, "PROXY:", proxyAddr)

	// _, err = proxies["redpanda"].AddToxic("debug_redpanda", "debug", "upstream", 1.0, toxiproxy.Attributes{})
	// assert.NoError(t, err)
	// defer proxies["redpanda"].RemoveToxic("debug_redpanda")

	_, err = proxies["redpanda"].AddToxic("parse_redpanda_upstream", "kafka_upstream_parse", "upstream", 1.0, toxiproxy.Attributes{})
	assert.NoError(t, err)
	defer proxies["redpanda"].RemoveToxic("parse_redpanda_upstream")

	_, err = proxies["redpanda"].AddToxic("parse_redpanda_downstream", "kafka_downstream_parse", "downstream", 1.0, toxiproxy.Attributes{})
	assert.NoError(t, err)
	defer proxies["redpanda"].RemoveToxic("parse_redpanda_downstream")

	proxiedClient, err := kgo.NewClient(
		kgo.SeedBrokers(proxyAddr),
	)

	req := kmsg.NewPtrMetadataRequest()
	topic := kmsg.NewMetadataRequestTopic()
	topic.Topic = kmsg.StringPtr("console_toxiproxy_test")
	req.Topics = append(req.Topics, topic)

	res, err := req.RequestWith(ctx, proxiedClient)
	assert.NoError(t, err)

	// Check response for Kafka error codes and print them.
	// Other requests might have top level error codes, which indicate completed but failed requests.
	for _, topic := range res.Topics {
		err := kerr.ErrorForCode(topic.ErrorCode)
		assert.NoError(t, err)
	}

	fmt.Printf("received '%v' topics and '%v' brokers\n", len(res.Topics), len(res.Brokers))

	assert.Fail(t, "FOO FAIL")
}

func runToxiproxyServer(t *testing.T, port int) {

	t.Helper()

	var err error
	timeout := 5 * time.Second

	portStr := strconv.Itoa(port)

	// Check if there is instance run
	conn, err := net.DialTimeout("tcp", "localhost:"+portStr, timeout)
	if err == nil {
		conn.Close()
		return
	}

	go func() {
		metricsContainer := toxiServer.NewMetricsContainer(prometheus.NewRegistry())
		server := toxiServer.NewServer(metricsContainer, zerolog.Nop())
		server.Listen("localhost", portStr)
	}()

	for i := 0; i < 10; i += 1 {
		conn, err := net.DialTimeout("tcp", "localhost:"+portStr, timeout)
		if err == nil {
			conn.Close()
			return
		}
	}

	require.NoError(t, err)
}

// DebugToxic prints bytes processed through pipe.
type DebugToxic struct{}

func (t *DebugToxic) PrintHex(data []byte) {
	for i := 0; i < len(data); {
		for j := 0; j < 4; j += 1 {
			x := i + 8
			if x >= len(data) {
				x = len(data) - 1
				fmt.Printf("% x\n", data[i:x])
				return
			}
			fmt.Printf("% x\t\t", data[i:x])
			i = x
		}
		fmt.Println()
	}
}

func (t *DebugToxic) Pipe(stub *toxics.ToxicStub) {
	buf := make([]byte, 32*1024)
	writer := stream.NewChanWriter(stub.Output)
	reader := stream.NewChanReader(stub.Input)
	reader.SetInterrupt(stub.Interrupt)

	for {
		n, err := reader.Read(buf)
		log.Printf("-- [DebugToxic] Processed %d bytes\n", n)

		if err == stream.ErrInterrupted {
			writer.Write(buf[:n])
			return
		} else if err == io.EOF {
			stub.Close()
			return
		}

		t.PrintHex(buf[:n])

		writer.Write(buf[:n])
	}
}

// KafkaToxic attempts to print out kafka messages.
type KafkaUpstreamToxic struct{}

func (t *KafkaUpstreamToxic) Pipe(stub *toxics.ToxicStub) {
	writer := stream.NewChanWriter(stub.Output)
	reader := stream.NewChanReader(stub.Input)
	reader.SetInterrupt(stub.Interrupt)

	for {
		// read size
		sizeBuf := make([]byte, 4)
		var err error
		n, err := io.ReadFull(reader, sizeBuf)

		if err == stream.ErrInterrupted {
			fmt.Println("Size ErrInterrupted")
			writer.Write(sizeBuf[:n])
			return
		} else if err == io.EOF {
			fmt.Println("Size EOF")
			stub.Close()
			return
		} else if err == io.ErrUnexpectedEOF {
			fmt.Println("Size ErrUnexpectedEOF")
			stub.Close()
			return
		}

		writer.Write(sizeBuf[:n])

		// read body
		size := int32(binary.BigEndian.Uint32(sizeBuf))
		body := make([]byte, size)
		n, err = io.ReadFull(reader, body)

		fmt.Printf("read: %+v asked: %+v body: %+v\n", n, size, len(body))

		parseKRequestMessage(body[:n])

		if err == stream.ErrInterrupted {
			fmt.Println("ErrInterrupted")
			writer.Write(body[:n])
			return
		} else if err == io.EOF {
			fmt.Println("EOF")
			stub.Close()
			return
		} else if err == io.ErrUnexpectedEOF {
			fmt.Println("ErrUnexpectedEOF")
			stub.Close()
			return
		}

		writer.Write(body[:n])
	}
}

func parseKRequestMessage(data []byte) {
	fmt.Println("body reading. body:", len(data))

	if len(data) > 0 {
		kreader := kbin.Reader{Src: data}
		key := kreader.Int16()
		version := kreader.Int16()
		corrID := kreader.Int32()
		_ = kreader.NullableString()
		kreq := kmsg.RequestForKey(key)

		fmt.Println("version:", version, "key:", key, "corrID:", corrID)

		kreq.SetVersion(version)
		if kreq.IsFlexible() {
			kmsg.SkipTags(&kreader)
		}
		if err := kreq.ReadFrom(kreader.Src); err != nil {
			fmt.Println("err reading request:", err.Error())
		} else {
			fmt.Println("read request with key:", kreq.Key())
			fmt.Printf("request type: %T\n", kreq)

			switch v := kreq.(type) {
			case *kmsg.MetadataRequest:
				topics := make([]string, len(v.Topics))
				for i, t := range v.Topics {
					t := t
					fmt.Println(*t.Topic)
					topics[i] = *t.Topic
				}
				topicsStr := strings.Join(topics, ",")
				fmt.Printf("kreq is metadata request for topic:%+v\n", topicsStr)
			default:
				fmt.Printf("kreq is unhandled type %T!\n", v)
			}

		}
	}
}

// KafkaDownstreamToxic attempts to print out kafka messages.
type KafkaDownstreamToxic struct{}

func (t *KafkaDownstreamToxic) Pipe(stub *toxics.ToxicStub) {
	writer := stream.NewChanWriter(stub.Output)
	reader := stream.NewChanReader(stub.Input)
	reader.SetInterrupt(stub.Interrupt)

	for {
		// read size
		sizeBuf := make([]byte, 4)
		var err error
		n, err := io.ReadFull(reader, sizeBuf)

		sizeBuffer := bytes.NewBuffer(sizeBuf)

		if err == stream.ErrInterrupted {
			fmt.Println("Size ErrInterrupted")
			sizeBuffer.WriteTo(writer)
			return
		} else if err == io.EOF {
			fmt.Println("Size EOF")
			stub.Close()
			return
		} else if err == io.ErrUnexpectedEOF {
			fmt.Println("Size ErrUnexpectedEOF")
			stub.Close()
			return
		}

		sizeBuffer.WriteTo(writer)

		// read body
		size := int32(binary.BigEndian.Uint32(sizeBuf))
		body := make([]byte, size)
		n, err = io.ReadFull(reader, body)

		fmt.Printf("read: %+v asked: %+v body: %+v\n", n, size, len(body))

		parseKResponseMessage(body[:n])

		bodyBuffer := bytes.NewBuffer(body)

		if err == stream.ErrInterrupted {
			fmt.Println("ErrInterrupted")
			bodyBuffer.WriteTo(writer)
			return
		} else if err == io.EOF {
			fmt.Println("EOF")
			stub.Close()
			return
		} else if err == io.ErrUnexpectedEOF {
			fmt.Println("ErrUnexpectedEOF")
			stub.Close()
			return
		}

		if err != nil {
			fmt.Println("ERR:", err.Error())
			bodyBuffer.WriteTo(writer)
		} else {
			fmt.Println("ELSE CASE")
			// t.ModifyResponse(resp)
			bodyBuffer.WriteTo(writer)
		}

		sizeBuffer.Reset()
		bodyBuffer.Reset()
	}
}

func parseKResponseMessage(data []byte) {
	fmt.Println("body reading. body:", len(data))

	if len(data) > 0 {
		kreader := kbin.Reader{Src: data}
		corrID := kreader.Int32()

		fmt.Println("corr:", corrID)

		kres := kmsg.ResponseForKey(3)
		kres.SetVersion(7)

		// kreader := kbin.Reader{Src: data}

		if err := kres.ReadFrom(kreader.Src); err != nil {
			fmt.Println("err reading response:", err.Error())
		} else {
			fmt.Println("read response with key:", kres.Key())
			fmt.Printf("response type: %T\n", kres)

			switch v := kres.(type) {
			case *kmsg.MetadataResponse:
				topics := make([]string, len(v.Topics))
				for i, t := range v.Topics {
					t := t
					fmt.Println(*t.Topic)
					topics[i] = *t.Topic
				}
				topicsStr := strings.Join(topics, ",")
				fmt.Printf("kres is metadata response for topic:%+v\n", topicsStr)
			default:
				fmt.Printf("kres is unhandled type %T!\n", v)
			}
		}
	}
}

func serializeResponse(r kmsg.Response, correlationID int32, clientID *string) []byte {
	dst := make([]byte, 0, 100)
	dst = append(dst, 0, 0, 0, 0) // reserve length
	k := r.Key()
	v := r.GetVersion()
	dst = kbin.AppendInt16(dst, k)
	dst = kbin.AppendInt16(dst, v)
	dst = kbin.AppendInt32(dst, correlationID)
	if k == 7 && v == 0 {
		return dst
	}

	// Even with flexible versions, we do not use a compact client id.
	// Clients issue ApiVersions immediately before knowing the broker
	// version, and old brokers will not be able to understand a compact
	// client id.
	dst = kbin.AppendNullableString(dst, clientID)

	// The flexible tags end the request header, and then begins the
	// request body.
	if r.IsFlexible() {
		var numTags uint8
		dst = append(dst, numTags)
		if numTags != 0 {
			// TODO when tags are added
		}
	}

	// Now the request body.
	dst = r.AppendTo(dst)

	kbin.AppendInt32(dst[:0], int32(len(dst[4:])))
	return dst
}
