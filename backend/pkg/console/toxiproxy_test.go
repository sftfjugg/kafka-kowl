//go:build integration

package console

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
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

	_, err = proxies["redpanda"].AddToxic("debug_redpanda", "debug", "downstream", 1.0, toxiproxy.Attributes{})
	assert.NoError(t, err)
	defer proxies["redpanda"].RemoveToxic("debug_redpanda")

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

	fmt.Printf("received '%v' topics and '%v' brokers", len(res.Topics), len(res.Brokers))

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
