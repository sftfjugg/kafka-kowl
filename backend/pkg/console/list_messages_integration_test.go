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
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

	defaultKafkaSvc, err := kafka.NewService(&cfg, log, cfg.MetricsNamespace)
	assert.NoError(t, err)

	kClient := defaultKafkaSvc.KafkaClient.(*kgo.Client)

	kafkaAdmCl := kadm.NewClient(kClient)

	defer defaultKafkaSvc.KafkaClient.Close()

	_, err = kafkaAdmCl.CreateTopic(ctx, 1, 1, nil, "console_list_messages_topic_test")
	assert.NoError(t, err)

	g := new(errgroup.Group)
	g.Go(func() error {
		produceOrders(t, ctx, kClient, "console_list_messages_topic_test")
		return nil
	})

	err = g.Wait()
	assert.NoError(t, err)

	type test struct {
		name          string
		setup         func(context.Context)
		input         *ListMessageRequest
		expect        func(*mocks.MockIListMessagesProgress, *mocks.MockClientRequestor)
		expectError   string
		cleanup       func(context.Context)
		useFakeClient bool
	}

	tests := []test{
		// {
		// 	name: "empty topic",
		// 	setup: func(ctx context.Context) {
		// 		_, err = kafkaAdmCl.CreateTopic(ctx, 1, 1, nil, "console_list_messages_empty_topic_test")
		// 		assert.NoError(t, err)
		// 	},
		// 	input: &ListMessageRequest{
		// 		TopicName:    "console_list_messages_empty_topic_test",
		// 		PartitionID:  -1,
		// 		StartOffset:  -2,
		// 		MessageCount: 100,
		// 	},
		// 	expect: func(mockProgress *mocks.MockIListMessagesProgress, mockRequestor *mocks.MockClientRequestor) {
		// 		mockProgress.EXPECT().OnPhase("Get Partitions")
		// 		mockProgress.EXPECT().OnPhase("Get Watermarks and calculate consuming requests")
		// 		mockProgress.EXPECT().OnComplete(gomock.Any(), false)
		// 	},
		// 	cleanup: func(ctx context.Context) {
		// 		kafkaAdmCl.DeleteTopics(ctx, "console_list_messages_empty_topic_test")
		// 	},
		// },
		// {
		// 	name: "all messages in a topic",
		// 	input: &ListMessageRequest{
		// 		TopicName:    "console_list_messages_topic_test",
		// 		PartitionID:  -1,
		// 		StartOffset:  -2,
		// 		MessageCount: 100,
		// 	},
		// 	expect: func(mockProgress *mocks.MockIListMessagesProgress, mockRequestor *mocks.MockClientRequestor) {
		// 		var msg *kafka.TopicMessage
		// 		var int64Type int64

		// 		mockProgress.EXPECT().OnPhase("Get Partitions")
		// 		mockProgress.EXPECT().OnPhase("Get Watermarks and calculate consuming requests")
		// 		mockProgress.EXPECT().OnPhase("Consuming messages")
		// 		mockProgress.EXPECT().OnMessage(gomock.AssignableToTypeOf(msg)).Times(20)
		// 		mockProgress.EXPECT().OnMessageConsumed(gomock.AssignableToTypeOf(int64Type)).Times(20)
		// 		mockProgress.EXPECT().OnComplete(gomock.AssignableToTypeOf(int64Type), false)
		// 	},
		// },
		{
			name: "single messages in a topic",
			input: &ListMessageRequest{
				TopicName:    "console_list_messages_topic_test",
				PartitionID:  -1,
				StartOffset:  10,
				MessageCount: 1,
			},
			expect: func(mockProgress *mocks.MockIListMessagesProgress, mockRequestor *mocks.MockClientRequestor) {
				var int64Type int64

				mockProgress.EXPECT().OnPhase("Get Partitions")
				mockProgress.EXPECT().OnPhase("Get Watermarks and calculate consuming requests")
				mockProgress.EXPECT().OnPhase("Consuming messages")
				mockProgress.EXPECT().OnMessage(matchesOrder("10")).Times(1)
				mockProgress.EXPECT().OnMessageConsumed(gomock.AssignableToTypeOf(int64Type)).Times(1)
				mockProgress.EXPECT().OnComplete(gomock.AssignableToTypeOf(int64Type), false)
			},
		},
		// {
		// 	name: "5 messages in a topic",
		// 	input: &ListMessageRequest{
		// 		TopicName:    "console_list_messages_topic_test",
		// 		PartitionID:  -1,
		// 		StartOffset:  10,
		// 		MessageCount: 5,
		// 	},
		// 	expect: func(mockProgress *mocks.MockIListMessagesProgress, mockRequestor *mocks.MockClientRequestor) {
		// 		var int64Type int64

		// 		mockProgress.EXPECT().OnPhase("Get Partitions")
		// 		mockProgress.EXPECT().OnPhase("Get Watermarks and calculate consuming requests")
		// 		mockProgress.EXPECT().OnPhase("Consuming messages")
		// 		mockProgress.EXPECT().OnMessage(matchesOrder("10")).Times(1)
		// 		mockProgress.EXPECT().OnMessage(matchesOrder("11")).Times(1)
		// 		mockProgress.EXPECT().OnMessage(matchesOrder("12")).Times(1)
		// 		mockProgress.EXPECT().OnMessage(matchesOrder("13")).Times(1)
		// 		mockProgress.EXPECT().OnMessage(matchesOrder("14")).Times(1)
		// 		mockProgress.EXPECT().OnMessageConsumed(gomock.AssignableToTypeOf(int64Type)).Times(5)
		// 		mockProgress.EXPECT().OnComplete(gomock.AssignableToTypeOf(int64Type), false)
		// 	},
		// },
		// {
		// 	name: "time stamp in future get last record",
		// 	input: &ListMessageRequest{
		// 		TopicName:      "console_list_messages_topic_test",
		// 		PartitionID:    -1,
		// 		MessageCount:   5,
		// 		StartTimestamp: time.Date(2010, time.November, 11, 13, 0, 0, 0, time.UTC).UnixMilli(),
		// 		StartOffset:    StartOffsetTimestamp,
		// 	},
		// 	expect: func(mockProgress *mocks.MockIListMessagesProgress, mockRequestor *mocks.MockClientRequestor) {
		// 		var int64Type int64

		// 		mockProgress.EXPECT().OnPhase("Get Partitions")
		// 		mockProgress.EXPECT().OnPhase("Get Watermarks and calculate consuming requests")
		// 		mockProgress.EXPECT().OnPhase("Consuming messages")
		// 		mockProgress.EXPECT().OnMessage(matchesOrder("19")).Times(1)
		// 		mockProgress.EXPECT().OnMessageConsumed(gomock.AssignableToTypeOf(int64Type)).Times(1)
		// 		mockProgress.EXPECT().OnComplete(gomock.AssignableToTypeOf(int64Type), false)
		// 	},
		// },
		// {
		// 	name: "time stamp in middle get 5 records",
		// 	input: &ListMessageRequest{
		// 		TopicName:      "console_list_messages_topic_test",
		// 		PartitionID:    -1,
		// 		MessageCount:   5,
		// 		StartTimestamp: time.Date(2010, time.November, 10, 13, 10, 30, 0, time.UTC).UnixMilli(),
		// 		StartOffset:    StartOffsetTimestamp,
		// 	},
		// 	expect: func(mockProgress *mocks.MockIListMessagesProgress, mockRequestor *mocks.MockClientRequestor) {
		// 		var int64Type int64

		// 		mockProgress.EXPECT().OnPhase("Get Partitions")
		// 		mockProgress.EXPECT().OnPhase("Get Watermarks and calculate consuming requests")
		// 		mockProgress.EXPECT().OnPhase("Consuming messages")
		// 		mockProgress.EXPECT().OnMessage(matchesOrder("11")).Times(1)
		// 		mockProgress.EXPECT().OnMessage(matchesOrder("12")).Times(1)
		// 		mockProgress.EXPECT().OnMessage(matchesOrder("13")).Times(1)
		// 		mockProgress.EXPECT().OnMessage(matchesOrder("14")).Times(1)
		// 		mockProgress.EXPECT().OnMessage(matchesOrder("15")).Times(1)
		// 		mockProgress.EXPECT().OnMessageConsumed(gomock.AssignableToTypeOf(int64Type)).Times(5)
		// 		mockProgress.EXPECT().OnComplete(gomock.AssignableToTypeOf(int64Type), false)
		// 	},
		// },
		// {
		// 	name: "unknown topic",
		// 	input: &ListMessageRequest{
		// 		TopicName:    "console_list_messages_topic_test_unknown_topic",
		// 		PartitionID:  -1,
		// 		StartOffset:  -2,
		// 		MessageCount: 100,
		// 	},
		// 	expect: func(mockProgress *mocks.MockIListMessagesProgress, mockRequestor *mocks.MockClientRequestor) {
		// 		mockProgress.EXPECT().OnPhase("Get Partitions")
		// 	},
		// 	expectError: "failed to get partitions: UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition.",
		// },
		{
			name:          "single messages in a topic mock",
			useFakeClient: true,
			input: &ListMessageRequest{
				TopicName:    "console_list_messages_topic_test_mock",
				PartitionID:  -1,
				StartOffset:  10,
				MessageCount: 1,
			},
			expect: func(mockProgress *mocks.MockIListMessagesProgress, mockRequestor *mocks.MockClientRequestor) {
				var int64Type int64

				mockProgress.EXPECT().OnPhase("Get Partitions")

				mdReq := kmsg.NewMetadataRequest()
				mdReq.Topics = make([]kmsg.MetadataRequestTopic, 1)
				topicReq := kmsg.NewMetadataRequestTopic()
				topicReq.Topic = kadm.StringPtr("console_list_messages_topic_test_mock")
				mdReq.Topics[0] = topicReq

				mockRequestor.EXPECT().Request(
					gomock.Any(),
					&mdReq,
				).Times(1).DoAndReturn(func(mockCtx context.Context, req *kmsg.MetadataRequest) (*kmsg.MetadataResponse, error) {
					mdRes := kmsg.NewMetadataResponse()
					mdRes.Version = 7
					mdRes.Topics = make([]kmsg.MetadataResponseTopic, 1)
					mdRes.Topics[0] = kmsg.NewMetadataResponseTopic()
					mdRes.Topics[0].Topic = kmsg.StringPtr("console_list_messages_topic_test_mock")
					mdRes.Topics[0].Partitions = make([]kmsg.MetadataResponseTopicPartition, 1)
					mdRes.Topics[0].Partitions[0].Partition = 0
					mdRes.Topics[0].Partitions[0].Leader = 0
					mdRes.Topics[0].Partitions[0].LeaderEpoch = 1
					mdRes.Topics[0].Partitions[0].Replicas = make([]int32, 1)
					mdRes.Topics[0].Partitions[0].Replicas[0] = 0
					mdRes.Topics[0].Partitions[0].ISR = make([]int32, 1)
					mdRes.Topics[0].Partitions[0].ISR[0] = 0

					return &mdRes, nil
				})

				// watermarks
				mockProgress.EXPECT().OnPhase("Get Watermarks and calculate consuming requests")

				// first sharded request
				loReq := kmsg.NewListOffsetsRequest()
				loReq.ReplicaID = 0
				loReq.Topics = make([]kmsg.ListOffsetsRequestTopic, 1)
				loReq.Topics[0] = kmsg.NewListOffsetsRequestTopic()
				loReq.Topics[0].Topic = "console_list_messages_topic_test_mock"
				loReq.Topics[0].Partitions = make([]kmsg.ListOffsetsRequestTopicPartition, 1)
				loReq.Topics[0].Partitions[0].Partition = 0
				loReq.Topics[0].Partitions[0].CurrentLeaderEpoch = -1
				loReq.Topics[0].Partitions[0].Timestamp = -2
				loReq.Topics[0].Partitions[0].MaxNumOffsets = 1

				// and response

				loReqForRes := kmsg.NewListOffsetsRequest()
				// hacky way to deep copy
				loReqJSON, err := json.Marshal(loReq)
				assert.NoError(t, err)
				json.Unmarshal(loReqJSON, &loReqForRes)
				loReqForRes.Version = 4

				loRes := kmsg.NewListOffsetsResponse()
				loRes.Version = 4
				loRes.Topics = make([]kmsg.ListOffsetsResponseTopic, 1)
				loRes.Topics[0] = kmsg.NewListOffsetsResponseTopic()
				loRes.Topics[0].Topic = "console_list_messages_topic_test_mock"
				loRes.Topics[0].Partitions = make([]kmsg.ListOffsetsResponseTopicPartition, 1)
				loRes.Topics[0].Partitions[0].Partition = 0
				loRes.Topics[0].Partitions[0].LeaderEpoch = 1
				loRes.Topics[0].Partitions[0].Timestamp = -1
				loRes.Topics[0].Partitions[0].Offset = 0

				mockRequestor.EXPECT().RequestSharded(
					gomock.Any(),
					&loReq,
				).Times(1).Return([]kgo.ResponseShard{
					{
						Req:  &loReqForRes,
						Resp: &loRes,
					},
				})

				// 2nd sharded request
				loReq2 := kmsg.NewListOffsetsRequest()
				// hacky way to deep copy
				loReqJSON, err = json.Marshal(loReq)
				assert.NoError(t, err)
				json.Unmarshal(loReqJSON, &loReq2)
				loReq2.Topics[0].Partitions[0].Timestamp = -1
				loReq2.Topics[0].Partitions[0].CurrentLeaderEpoch = -1
				loReq2.Topics[0].Partitions[0].MaxNumOffsets = 1

				loReq2ForRes := kmsg.NewListOffsetsRequest()
				loReqJSON, err = json.Marshal(loReq2)
				assert.NoError(t, err)
				json.Unmarshal(loReqJSON, &loReq2ForRes)
				loReq2ForRes.Version = 4

				// 2nd response
				loRes2 := kmsg.NewListOffsetsResponse()
				loResJSON, err := json.Marshal(loRes)
				assert.NoError(t, err)
				json.Unmarshal(loResJSON, &loRes2)
				loRes2.Topics[0].Partitions[0].Timestamp = -1
				loRes2.Topics[0].Partitions[0].Offset = 20
				loRes2.Topics[0].Partitions[0].LeaderEpoch = 1

				mockRequestor.EXPECT().RequestSharded(
					gomock.Any(),
					&loReq2,
				).Times(1).Return([]kgo.ResponseShard{
					{
						Req:  &loReq2ForRes,
						Resp: &loRes2,
					},
				})

				mockProgress.EXPECT().OnPhase("Consuming messages")

				mockProgress.EXPECT().OnMessage(matchesOrder("10")).Times(1)

				mockProgress.EXPECT().OnMessageConsumed(gomock.AssignableToTypeOf(int64Type)).Times(1)

				mockProgress.EXPECT().OnComplete(gomock.AssignableToTypeOf(int64Type), false)
			},
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

			mockClient := mocks.NewMockClientRequestor(mockCtrl)
			mockClientGenerator := func(...kgo.Opt) (kafka.ClientRequestor, error) {
				return mockClient, nil
			}

			if tc.expect != nil {
				tc.expect(mockProgress, mockClient)
			}

			var kafkaSvc *kafka.Service

			kafkaSvc = defaultKafkaSvc
			if tc.useFakeClient {
				fmt.Println()
				fmt.Println("!!! MOCK TEST !!!")
				fmt.Println()

				metricName := strings.ReplaceAll(tc.name, " ", "")
				kafkaSvc, err = kafka.NewService(&cfg, log, metricName,
					kafka.WithClient(mockClient),
					kafka.WithClientGenerator(mockClientGenerator),
				)

			}

			svc, err := NewService(cfg.Console, log, kafkaSvc, nil, nil)
			assert.NoError(t, err)

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
