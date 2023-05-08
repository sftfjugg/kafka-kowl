// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file https://github.com/redpanda-data/redpanda/blob/dev/licenses/bsl.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build integration

package console

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/console/backend/pkg/config"
	"github.com/redpanda-data/console/backend/pkg/kafka"
	"github.com/redpanda-data/console/backend/pkg/kafka/mocks"
)

func Test_ListMessages(t *testing.T) {
	ctx := context.Background()
	log, err := zap.NewProduction()
	assert.NoError(t, err)

	cfg := config.Config{}
	cfg.SetDefaults()

	cfg.MetricsNamespace = "console_list_messages"
	cfg.Kafka.Brokers = []string{testSeedBroker}

	kafkaSvc, err := kafka.NewService(&cfg, log, cfg.MetricsNamespace)
	assert.NoError(t, err)

	svc, err := NewService(cfg.Console, log, kafkaSvc, nil, nil)
	assert.NoError(t, err)

	kafkaAdmCl := kadm.NewClient(svc.kafkaSvc.KafkaClient)

	defer svc.kafkaSvc.KafkaClient.Close()

	_, err = kafkaAdmCl.CreateTopic(ctx, 1, 1, nil, "console_list_messages_topic_test")
	assert.NoError(t, err)

	g := new(errgroup.Group)
	g.Go(func() error {
		produceOrders(t, ctx, svc.kafkaSvc.KafkaClient, "console_list_messages_topic_test")
		return nil
	})

	err = g.Wait()
	assert.NoError(t, err)

	type test struct {
		name        string
		setup       func(context.Context)
		input       *ListMessageRequest
		expect      func(*mocks.MockIListMessagesProgress)
		expectError string
		cleanup     func(context.Context)
	}

	tests := []test{
		{
			name: "empty topic",
			setup: func(ctx context.Context) {
				_, err = kafkaAdmCl.CreateTopic(ctx, 1, 1, nil, "console_list_messages_empty_topic_test")
				assert.NoError(t, err)
			},
			input: &ListMessageRequest{
				TopicName:    "console_list_messages_empty_topic_test",
				PartitionID:  -1,
				StartOffset:  -2,
				MessageCount: 100,
			},
			expect: func(mockProgress *mocks.MockIListMessagesProgress) {
				mockProgress.EXPECT().OnPhase("Get Partitions")
				mockProgress.EXPECT().OnPhase("Get Watermarks and calculate consuming requests")
				mockProgress.EXPECT().OnComplete(gomock.Any(), false)
			},
			cleanup: func(ctx context.Context) {
				kafkaAdmCl.DeleteTopics(ctx, "console_list_messages_empty_topic_test")
			},
		},
		{
			name: "all messages in a topic",
			input: &ListMessageRequest{
				TopicName:    "console_list_messages_topic_test",
				PartitionID:  -1,
				StartOffset:  -2,
				MessageCount: 100,
			},
			expect: func(mockProgress *mocks.MockIListMessagesProgress) {
				var msg *kafka.TopicMessage
				var int64Type int64

				mockProgress.EXPECT().OnPhase("Get Partitions")
				mockProgress.EXPECT().OnPhase("Get Watermarks and calculate consuming requests")
				mockProgress.EXPECT().OnPhase("Consuming messages")
				mockProgress.EXPECT().OnMessage(gomock.AssignableToTypeOf(msg)).Times(20)
				mockProgress.EXPECT().OnMessageConsumed(gomock.AssignableToTypeOf(int64Type)).Times(20)
				mockProgress.EXPECT().OnComplete(gomock.AssignableToTypeOf(int64Type), false)
			},
		},
		{
			name: "single messages in a topic",
			input: &ListMessageRequest{
				TopicName:    "console_list_messages_topic_test",
				PartitionID:  -1,
				StartOffset:  10,
				MessageCount: 1,
			},
			expect: func(mockProgress *mocks.MockIListMessagesProgress) {
				var int64Type int64

				mockProgress.EXPECT().OnPhase("Get Partitions")
				mockProgress.EXPECT().OnPhase("Get Watermarks and calculate consuming requests")
				mockProgress.EXPECT().OnPhase("Consuming messages")
				mockProgress.EXPECT().OnMessage(matchesOrder("10")).Times(1)
				mockProgress.EXPECT().OnMessageConsumed(gomock.AssignableToTypeOf(int64Type)).Times(1)
				mockProgress.EXPECT().OnComplete(gomock.AssignableToTypeOf(int64Type), false)
			},
		},
		{
			name: "5 messages in a topic",
			input: &ListMessageRequest{
				TopicName:    "console_list_messages_topic_test",
				PartitionID:  -1,
				StartOffset:  10,
				MessageCount: 5,
			},
			expect: func(mockProgress *mocks.MockIListMessagesProgress) {
				var int64Type int64

				mockProgress.EXPECT().OnPhase("Get Partitions")
				mockProgress.EXPECT().OnPhase("Get Watermarks and calculate consuming requests")
				mockProgress.EXPECT().OnPhase("Consuming messages")
				mockProgress.EXPECT().OnMessage(matchesOrder("10")).Times(1)
				mockProgress.EXPECT().OnMessage(matchesOrder("11")).Times(1)
				mockProgress.EXPECT().OnMessage(matchesOrder("12")).Times(1)
				mockProgress.EXPECT().OnMessage(matchesOrder("13")).Times(1)
				mockProgress.EXPECT().OnMessage(matchesOrder("14")).Times(1)
				mockProgress.EXPECT().OnMessageConsumed(gomock.AssignableToTypeOf(int64Type)).Times(5)
				mockProgress.EXPECT().OnComplete(gomock.AssignableToTypeOf(int64Type), false)
			},
		},
		{
			name: "time stamp in future get last record",
			input: &ListMessageRequest{
				TopicName:      "console_list_messages_topic_test",
				PartitionID:    -1,
				MessageCount:   5,
				StartTimestamp: time.Date(2010, time.November, 11, 13, 0, 0, 0, time.UTC).UnixMilli(),
				StartOffset:    StartOffsetTimestamp,
			},
			expect: func(mockProgress *mocks.MockIListMessagesProgress) {
				var int64Type int64

				mockProgress.EXPECT().OnPhase("Get Partitions")
				mockProgress.EXPECT().OnPhase("Get Watermarks and calculate consuming requests")
				mockProgress.EXPECT().OnPhase("Consuming messages")
				mockProgress.EXPECT().OnMessage(matchesOrder("19")).Times(1)
				mockProgress.EXPECT().OnMessageConsumed(gomock.AssignableToTypeOf(int64Type)).Times(1)
				mockProgress.EXPECT().OnComplete(gomock.AssignableToTypeOf(int64Type), false)
			},
		},
		{
			name: "time stamp in middle get 5 records",
			input: &ListMessageRequest{
				TopicName:      "console_list_messages_topic_test",
				PartitionID:    -1,
				MessageCount:   5,
				StartTimestamp: time.Date(2010, time.November, 10, 13, 10, 30, 0, time.UTC).UnixMilli(),
				StartOffset:    StartOffsetTimestamp,
			},
			expect: func(mockProgress *mocks.MockIListMessagesProgress) {
				var int64Type int64

				mockProgress.EXPECT().OnPhase("Get Partitions")
				mockProgress.EXPECT().OnPhase("Get Watermarks and calculate consuming requests")
				mockProgress.EXPECT().OnPhase("Consuming messages")
				mockProgress.EXPECT().OnMessage(matchesOrder("11")).Times(1)
				mockProgress.EXPECT().OnMessage(matchesOrder("12")).Times(1)
				mockProgress.EXPECT().OnMessage(matchesOrder("13")).Times(1)
				mockProgress.EXPECT().OnMessage(matchesOrder("14")).Times(1)
				mockProgress.EXPECT().OnMessage(matchesOrder("15")).Times(1)
				mockProgress.EXPECT().OnMessageConsumed(gomock.AssignableToTypeOf(int64Type)).Times(5)
				mockProgress.EXPECT().OnComplete(gomock.AssignableToTypeOf(int64Type), false)
			},
		},
		{
			name: "unknown topic",
			input: &ListMessageRequest{
				TopicName:    "console_list_messages_topic_test_unknown_topic",
				PartitionID:  -1,
				StartOffset:  -2,
				MessageCount: 100,
			},
			expect: func(mockProgress *mocks.MockIListMessagesProgress) {
				mockProgress.EXPECT().OnPhase("Get Partitions")
			},
			expectError: "failed to get partitions: UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition.",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			mockProgress := mocks.NewMockIListMessagesProgress(mockCtrl)

			if tc.setup != nil {
				tc.setup(ctx)
			}

			if tc.expect != nil {
				tc.expect(mockProgress)
			}

			err = svc.ListMessages(ctx, *tc.input, mockProgress)
			if tc.expectError != "" {
				assert.Error(t, err)
				assert.Equal(t, tc.expectError, err.Error())
			} else {
				assert.NoError(t, err)
			}

			if tc.cleanup != nil {
				tc.cleanup(ctx)
			}
		})
	}
}

type Order struct {
	ID string
}

type orderMatcher struct {
	expectedID string
	actualID   string
	err        string
}

func (om *orderMatcher) Matches(x interface{}) bool {
	if m, ok := x.(*kafka.TopicMessage); ok {
		order := Order{}
		err := json.Unmarshal(m.Value.Payload.Payload, &order)
		if err != nil {
			om.err = fmt.Sprintf("marshal error: %s", err.Error())
			return false
		}

		om.actualID = order.ID

		return order.ID == om.expectedID
	}

	om.err = "value is not a TopicMessage"
	return false
}

func (m *orderMatcher) String() string {
	return fmt.Sprintf("has order ID %s expected order ID %s. err: %s", m.actualID, m.expectedID, m.err)
}

func matchesOrder(id string) gomock.Matcher {
	return &orderMatcher{expectedID: id}
}

func produceOrders(t *testing.T, ctx context.Context, kafkaCl *kgo.Client, topic string) {
	t.Helper()

	ticker := time.NewTicker(100 * time.Millisecond)

	recordTimeStamp := time.Date(2010, time.November, 10, 13, 0, 0, 0, time.UTC)

	i := 0
	for i < 20 {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			order := Order{ID: strconv.Itoa(i)}
			serializedOrder, err := json.Marshal(order)
			require.NoError(t, err)

			r := &kgo.Record{
				Key:       []byte(order.ID),
				Value:     serializedOrder,
				Topic:     topic,
				Timestamp: recordTimeStamp,
			}
			results := kafkaCl.ProduceSync(ctx, r)
			require.NoError(t, results.FirstErr())

			fmt.Println("produced:", i)

			i++
			recordTimeStamp = recordTimeStamp.Add(1 * time.Minute)
		}
	}
}
