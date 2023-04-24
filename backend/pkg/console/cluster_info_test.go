package console

import (
	"context"
	"net"
	"testing"

	"github.com/redpanda-data/console/backend/pkg/config"
	"github.com/redpanda-data/console/backend/pkg/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"go.uber.org/zap"
)

func Test_GetClusterInfo(t *testing.T) {
	ctx := context.Background()
	container, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	seedBroker, err := container.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	log, err := zap.NewDevelopment()
	require.NoError(t, err)

	cfg := config.Config{}
	cfg.SetDefaults()
	cfg.Kafka.Brokers = []string{seedBroker}

	kafkaSvc, err := kafka.NewService(&cfg, log, cfg.MetricsNamespace)
	require.NoError(t, err)

	svc, err := NewService(cfg.Console, log, kafkaSvc, nil, nil)
	require.NoError(t, err)

	info, err := svc.GetClusterInfo(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, info)

	assert.Equal(t, "unknown custom version at least v0.10.2", info.KafkaVersion)
	assert.Len(t, info.Brokers, 1)
	assert.NotEmpty(t, info.Brokers[0])

	expectedAddr, expectedPort, err := net.SplitHostPort(seedBroker)
	assert.NoError(t, err)

	actualAddr, actualPort, err := net.SplitHostPort(seedBroker)
	assert.NoError(t, err)

	assert.Equal(t, expectedAddr, actualAddr)
	assert.Equal(t, expectedPort, actualPort)
}
