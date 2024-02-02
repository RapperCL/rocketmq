/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.processor;

import com.alibaba.fastjson.JSON;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.longpolling.PollingHeader;
import org.apache.rocketmq.broker.longpolling.PollingResult;
import org.apache.rocketmq.broker.longpolling.PopLongPollingService;
import org.apache.rocketmq.broker.longpolling.PopRequest;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_RETRY;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;

public class PopMessageProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER =
        LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    private final Random random = new Random(System.currentTimeMillis());
    String reviveTopic;
    private static final String BORN_TIME = "bornTime";

    private final PopLongPollingService popLongPollingService;
    private final PopBufferMergeService popBufferMergeService;
    private final QueueLockManager queueLockManager;
    private final AtomicLong ckMessageNumber;

    public PopMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.reviveTopic = PopAckConstants.buildClusterReviveTopic(this.brokerController.getBrokerConfig().getBrokerClusterName());
        this.popLongPollingService = new PopLongPollingService(brokerController, this);
        this.queueLockManager = new QueueLockManager();
        this.popBufferMergeService = new PopBufferMergeService(this.brokerController, this);
        this.ckMessageNumber = new AtomicLong();
    }

    public PopLongPollingService getPopLongPollingService() {
        return popLongPollingService;
    }

    public PopBufferMergeService getPopBufferMergeService() {
        return this.popBufferMergeService;
    }

    public QueueLockManager getQueueLockManager() {
        return queueLockManager;
    }

    public static String genAckUniqueId(AckMsg ackMsg) {
        return ackMsg.getTopic()
            + PopAckConstants.SPLIT + ackMsg.getQueueId()
            + PopAckConstants.SPLIT + ackMsg.getAckOffset()
            + PopAckConstants.SPLIT + ackMsg.getConsumerGroup()
            + PopAckConstants.SPLIT + ackMsg.getPopTime()
            + PopAckConstants.SPLIT + ackMsg.getBrokerName()
            + PopAckConstants.SPLIT + PopAckConstants.ACK_TAG;
    }

    public static String genBatchAckUniqueId(BatchAckMsg batchAckMsg) {
        return batchAckMsg.getTopic()
                + PopAckConstants.SPLIT + batchAckMsg.getQueueId()
                + PopAckConstants.SPLIT + batchAckMsg.getAckOffsetList().toString()
                + PopAckConstants.SPLIT + batchAckMsg.getConsumerGroup()
                + PopAckConstants.SPLIT + batchAckMsg.getPopTime()
                + PopAckConstants.SPLIT + PopAckConstants.BATCH_ACK_TAG;
    }

    public static String genCkUniqueId(PopCheckPoint ck) {
        return ck.getTopic()
            + PopAckConstants.SPLIT + ck.getQueueId()
            + PopAckConstants.SPLIT + ck.getStartOffset()
            + PopAckConstants.SPLIT + ck.getCId()
            + PopAckConstants.SPLIT + ck.getPopTime()
            + PopAckConstants.SPLIT + ck.getBrokerName()
            + PopAckConstants.SPLIT + PopAckConstants.CK_TAG;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> getPollingMap() {
        return popLongPollingService.getPollingMap();
    }

    public void notifyLongPollingRequestIfNeed(String topic, String group, int queueId) {
        long popBufferOffset = this.brokerController.getPopMessageProcessor().getPopBufferMergeService().getLatestOffset(topic, group, queueId);
        long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, queueId);
        long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
        long offset = Math.max(popBufferOffset, consumerOffset);
        if (maxOffset > offset) {
            boolean notifySuccess = popLongPollingService.notifyMessageArriving(topic, group, -1);
            if (!notifySuccess) {
                // notify pop queue
                notifySuccess = popLongPollingService.notifyMessageArriving(topic, group, queueId);
            }
            this.brokerController.getNotificationProcessor().notifyMessageArriving(topic, queueId);
            if (this.brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("notify long polling request. topic:{}, group:{}, queueId:{}, success:{}",
                    topic, group, queueId, notifySuccess);
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId) {
        popLongPollingService.notifyMessageArriving(topic, queueId);
    }

    public boolean notifyMessageArriving(final String topic, final String cid, final int queueId) {
        return popLongPollingService.notifyMessageArriving(topic, cid, queueId);
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        Optional.ofNullable(request.getExtFields().get(BORN_TIME)).ifPresent(s->{
            if (Objects.equals(request.getExtFields().get(BORN_TIME), "0")) {
                request.addExtField(BORN_TIME, String.valueOf(System.currentTimeMillis()));
            }
        });
        Channel channel = ctx.channel();

        RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
        final PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();
        final PopMessageRequestHeader requestHeader =
            (PopMessageRequestHeader) request.decodeCommandCustomHeader(PopMessageRequestHeader.class);
        StringBuilder startOffsetInfo = new StringBuilder(64);
        StringBuilder msgOffsetInfo = new StringBuilder(64);
        StringBuilder orderCountInfo = null;
        if (requestHeader.isOrder()) {
            orderCountInfo = new StringBuilder(64);
        }

        brokerController.getConsumerManager().compensateBasicConsumerInfo(requestHeader.getConsumerGroup(),
            ConsumeType.CONSUME_POP, MessageModel.CLUSTERING);

        response.setOpaque(request.getOpaque());

        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("receive PopMessage request command, {}", request);
        }

        if (requestHeader.isTimeoutTooMuch()) {
            response.setCode(ResponseCode.POLLING_TIMEOUT);
            response.setRemark(String.format("the broker[%s] pop message is timeout too much",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }
        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] pop message is forbidden",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }
        if (requestHeader.getMaxMsgNums() > 32) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("the broker[%s] pop message's num is greater than 32",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        if (!brokerController.getMessageStore().getMessageStoreConfig().isTimerWheelEnable()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("the broker[%s] pop message is forbidden because timerWheelEnable is false",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        TopicConfig topicConfig =
            this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(),
                RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(),
                FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] peeking message is forbidden");
            return response;
        }

        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] " +
                    "consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(),
                channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }
        SubscriptionGroupConfig subscriptionGroupConfig =
            this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        // subscriptionGroupConfig 不可能为空
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s",
                requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        ExpressionMessageFilter messageFilter = null;
        if (requestHeader.getExp() != null && requestHeader.getExp().length() > 0) {
            try {
                SubscriptionData subscriptionData = FilterAPI.build(requestHeader.getTopic(), requestHeader.getExp(), requestHeader.getExpType());
                brokerController.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(),
                    requestHeader.getTopic(), subscriptionData);

                String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                SubscriptionData retrySubscriptionData = FilterAPI.build(retryTopic, SubscriptionData.SUB_ALL, requestHeader.getExpType());
                brokerController.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(),
                    retryTopic, retrySubscriptionData);

                ConsumerFilterData consumerFilterData = null;
                if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    consumerFilterData = ConsumerFilterManager.build(
                        requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getExp(),
                        requestHeader.getExpType(), System.currentTimeMillis()
                    );
                    if (consumerFilterData == null) {
                        POP_LOGGER.warn("Parse the consumer's subscription[{}] failed, group: {}",
                            requestHeader.getExp(), requestHeader.getConsumerGroup());
                        response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                        response.setRemark("parse the consumer's subscription failed");
                        return response;
                    }
                }
                messageFilter = new ExpressionMessageFilter(subscriptionData, consumerFilterData,
                    brokerController.getConsumerFilterManager());
            } catch (Exception e) {
                POP_LOGGER.warn("Parse the consumer's subscription[{}] error, group: {}", requestHeader.getExp(),
                    requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }
        } else {
            try {
                SubscriptionData subscriptionData = FilterAPI.build(requestHeader.getTopic(), "*", ExpressionType.TAG);
                brokerController.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(),
                    requestHeader.getTopic(), subscriptionData);

                String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                SubscriptionData retrySubscriptionData = FilterAPI.build(retryTopic, "*", ExpressionType.TAG);
                brokerController.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(),
                    retryTopic, retrySubscriptionData);
            } catch (Exception e) {
                POP_LOGGER.warn("Build default subscription error, group: {}", requestHeader.getConsumerGroup());
            }
        }
        int reviveQid;
        if (requestHeader.isOrder()) {
            reviveQid = KeyBuilder.POP_ORDER_REVIVE_QUEUE;
        } else {
            reviveQid = (int) Math.abs(ckMessageNumber.getAndIncrement() % this.brokerController.getBrokerConfig().getReviveQueueNum());
        }

        int commercialSizePerMsg = this.brokerController.getBrokerConfig().getCommercialSizePerMsg();
        GetMessageResult getMessageResult = new GetMessageResult(commercialSizePerMsg);
        ExpressionMessageFilter finalMessageFilter = messageFilter;
        StringBuilder finalOrderCountInfo = orderCountInfo;
        // 随机选择从重试主题下，拉取消息？
        // 取随机数0-99
        int randomQ = random.nextInt(100);
        boolean needRetry = randomQ % 5 == 0;
        long popTime = System.currentTimeMillis();
        CompletableFuture<Long> getMessageFuture = CompletableFuture.completedFuture(0L);
        if (needRetry && !requestHeader.isOrder()) {
            TopicConfig retryTopicConfig =
                this.brokerController.getTopicConfigManager().selectTopicConfig(KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup()));
            if (retryTopicConfig != null) {
                for (int i = 0; i < retryTopicConfig.getReadQueueNums(); i++) {
                    int queueId = (randomQ + i) % retryTopicConfig.getReadQueueNums();
                    // 有没控制，拉取多少消息的数量
                    // 从重试主题的每个队列中拉取消息
                    // todo 有没有控制拉取的消息数，如果重试主题消息很多，会导致多次一直拉取,
                    // 有的，在拉取时，如果拉取的消息数达到了最大值，就不会继续拉取了，只会计算剩余的消息数
                    getMessageFuture = getMessageFuture.thenCompose(restNum -> popMsgFromQueue(requestHeader.getAttemptId(), true, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, finalMessageFilter,
                        startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
                }
            }
        }
        if (requestHeader.getQueueId() < 0) {
            // read all queue
            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                int queueId = (randomQ + i) % topicConfig.getReadQueueNums();
                // 拉取达到最大数量之后，不会继续拉取了，只会持续计算剩余的消息数
                getMessageFuture = getMessageFuture.thenCompose(restNum -> popMsgFromQueue(requestHeader.getAttemptId(), false, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, finalMessageFilter,
                    startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
            }
        } else {
            int queueId = requestHeader.getQueueId();
            getMessageFuture = getMessageFuture.thenCompose(restNum -> popMsgFromQueue(requestHeader.getAttemptId(), false, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, finalMessageFilter,
                startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
        }
        // if not full , fetch retry again
        if (!needRetry && getMessageResult.getMessageMapedList().size() < requestHeader.getMaxMsgNums() && !requestHeader.isOrder()) {
            TopicConfig retryTopicConfig =
                this.brokerController.getTopicConfigManager().selectTopicConfig(KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup()));
            if (retryTopicConfig != null) {
                for (int i = 0; i < retryTopicConfig.getReadQueueNums(); i++) {
                    int queueId = (randomQ + i) % retryTopicConfig.getReadQueueNums();
                    // 拉取达到最大数量之后，不会继续拉取了，只会持续计算剩余的消息数
                    getMessageFuture = getMessageFuture.thenCompose(restNum -> popMsgFromQueue(requestHeader.getAttemptId(), true, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, finalMessageFilter,
                        startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
                }
            }
        }

        final RemotingCommand finalResponse = response;
        getMessageFuture.thenApply(restNum -> {
            if (!getMessageResult.getMessageBufferList().isEmpty()) {
                finalResponse.setCode(ResponseCode.SUCCESS);
                getMessageResult.setStatus(GetMessageStatus.FOUND);
                if (restNum > 0) {
                    // 有多的消息时，通知对应的长轮询返回，并不在意restNum的多少
                    // all queue pop can not notify specified queue pop, and vice versa
                    popLongPollingService.notifyMessageArriving(requestHeader.getTopic(), requestHeader.getConsumerGroup(),
                        requestHeader.getQueueId());
                }
            } else {
                PollingResult pollingResult = popLongPollingService.polling(ctx, request, new PollingHeader(requestHeader));
                if (PollingResult.POLLING_SUC == pollingResult) {
                    return null;
                } else if (PollingResult.POLLING_FULL == pollingResult) {
                    finalResponse.setCode(ResponseCode.POLLING_FULL);
                } else {
                    finalResponse.setCode(ResponseCode.POLLING_TIMEOUT);
                }
                getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            }
            responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
            responseHeader.setPopTime(popTime);
            responseHeader.setReviveQid(reviveQid);
            responseHeader.setRestNum(restNum);
            responseHeader.setStartOffsetInfo(startOffsetInfo.toString());
            responseHeader.setMsgOffsetInfo(msgOffsetInfo.toString());
            if (requestHeader.isOrder() && finalOrderCountInfo != null) {
                responseHeader.setOrderCountInfo(finalOrderCountInfo.toString());
            }
            finalResponse.setRemark(getMessageResult.getStatus().name());
            switch (finalResponse.getCode()) {
                case ResponseCode.SUCCESS:
                    if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                        final long beginTimeMills = this.brokerController.getMessageStore().now();
                        final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(),
                            requestHeader.getTopic(), requestHeader.getQueueId());
                        this.brokerController.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(),
                            requestHeader.getTopic(), requestHeader.getQueueId(),
                            (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                        finalResponse.setBody(r);
                    } else {
                        final GetMessageResult tmpGetMessageResult = getMessageResult;
                        try {
                            FileRegion fileRegion =
                                new ManyMessageTransfer(finalResponse.encodeHeader(getMessageResult.getBufferTotalSize()),
                                    getMessageResult);
                            channel.writeAndFlush(fileRegion)
                                .addListener((ChannelFutureListener) future -> {
                                    tmpGetMessageResult.release();
                                    Attributes attributes = RemotingMetricsManager.newAttributesBuilder()
                                        .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
                                        .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(finalResponse.getCode()))
                                        .put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future))
                                        .build();
                                    RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);
                                    if (!future.isSuccess()) {
                                        POP_LOGGER.error("Fail to transfer messages from page cache to {}",
                                            channel.remoteAddress(), future.cause());
                                    }
                                });
                        } catch (Throwable e) {
                            POP_LOGGER.error("Error occurred when transferring messages from page cache", e);
                            getMessageResult.release();
                        }

                        return null;
                    }
                    break;
                default:
                    return finalResponse;
            }
            return finalResponse;
        }).thenAccept(result -> NettyRemotingAbstract.writeResponse(channel, request, result));
        return null;
    }

    private CompletableFuture<Long> popMsgFromQueue(String attemptId, boolean isRetry, GetMessageResult getMessageResult,
        PopMessageRequestHeader requestHeader, int queueId, long restNum, int reviveQid,
        Channel channel, long popTime, ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder orderCountInfo) {
        String topic = isRetry ? KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(),
            requestHeader.getConsumerGroup()) : requestHeader.getTopic();
        // 拉取时，进行加锁，避免并发重复拉取
        String lockKey =
            topic + PopAckConstants.SPLIT + requestHeader.getConsumerGroup() + PopAckConstants.SPLIT + queueId;
        boolean isOrder = requestHeader.isOrder();
        long offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(),
            false, lockKey, false);
        CompletableFuture<Long> future = new CompletableFuture<>();
        if (!queueLockManager.tryLock(lockKey)) {
            // restNum 应该是这段时间内又新增的数量，也就是这段时间内，还有未消费的消息
            restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
            future.complete(restNum);
            return future;
        }

        try {
            future.whenComplete((result, throwable) -> queueLockManager.unLock(lockKey));
            // 获取指定队列的pop消费位移
            offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(),
                true, lockKey, true);
            if (isOrder && brokerController.getConsumerOrderInfoManager().checkBlock(attemptId, topic,
                requestHeader.getConsumerGroup(), queueId, requestHeader.getInvisibleTime())) {
                future.complete(this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum);
                return future;
            }

            if (isOrder) {
                this.brokerController.getPopInflightMessageCounter().clearInFlightMessageNum(
                    topic,
                    requestHeader.getConsumerGroup(),
                    queueId
                );
            }
            // 拉取数量达到最大值后，停止拉取
            if (getMessageResult.getMessageMapedList().size() >= requestHeader.getMaxMsgNums()) {
                restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
                future.complete(restNum);
                return future;
            }
        } catch (Exception e) {
            POP_LOGGER.error("Exception in popMsgFromQueue", e);
            future.complete(restNum);
            return future;
        }

        AtomicLong atomicRestNum = new AtomicLong(restNum);
        AtomicLong atomicOffset = new AtomicLong(offset);
        long finalOffset = offset;
        // 根据offset获取消息
        return this.brokerController.getMessageStore()
            .getMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, offset,
                requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), messageFilter)
            .thenCompose(result -> {
                if (result == null) {
                    return CompletableFuture.completedFuture(null);
                }
                // maybe store offset is not correct.
                if (GetMessageStatus.OFFSET_TOO_SMALL.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())) {
                    // commit offset, because the offset is not correct
                    // If offset in store is greater than cq offset, it will cause duplicate messages,
                    // because offset in PopBuffer is not committed.
                    POP_LOGGER.warn("Pop initial offset, because store is no correct, {}, {}->{}",
                        lockKey, atomicOffset.get(), result.getNextBeginOffset());
                    this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic,
                        queueId, result.getNextBeginOffset());
                    atomicOffset.set(result.getNextBeginOffset());
                    // MessageStore使用了责任链设计模式，查找时优先从默认的存储中查找，查询不到时走多级存储查询。
                    return this.brokerController.getMessageStore().getMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, atomicOffset.get(),
                        requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), messageFilter);
                }
                return CompletableFuture.completedFuture(result);
            }).thenApply(result -> {
                if (result == null) {
                    atomicRestNum.set(brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - atomicOffset.get() + atomicRestNum.get());
                    return atomicRestNum.get();
                }
                if (!result.getMessageMapedList().isEmpty()) {
                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(requestHeader.getTopic(), result.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), topic,
                        result.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), topic,
                        result.getBufferTotalSize());

                    Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                        .put(LABEL_TOPIC, requestHeader.getTopic())
                        .put(LABEL_CONSUMER_GROUP, requestHeader.getConsumerGroup())
                        .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(requestHeader.getTopic()) || MixAll.isSysConsumerGroup(requestHeader.getConsumerGroup()))
                        .put(LABEL_IS_RETRY, isRetry)
                        .build();
                    BrokerMetricsManager.messagesOutTotal.add(result.getMessageCount(), attributes);
                    BrokerMetricsManager.throughputOutTotal.add(result.getBufferTotalSize(), attributes);

                    if (isOrder) {
                        this.brokerController.getConsumerOrderInfoManager().update(requestHeader.getAttemptId(), isRetry, topic,
                            requestHeader.getConsumerGroup(),
                            queueId, popTime, requestHeader.getInvisibleTime(), result.getMessageQueueOffset(),
                            orderCountInfo);
                        this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(),
                            requestHeader.getConsumerGroup(), topic, queueId, finalOffset);
                    } else {
                        // 将checkPoint添加到内存中，用于等待ack， 每拉取一批消息都会构建对应的CheckPoint，
                        // 相当于以队列的粒度。
                        if (!appendCheckPoint(requestHeader, topic, reviveQid, queueId, finalOffset, result, popTime, this.brokerController.getBrokerConfig().getBrokerName())) {
                            return atomicRestNum.get() + result.getMessageCount();
                        }
                    }
                    ExtraInfoUtil.buildStartOffsetInfo(startOffsetInfo, isRetry, queueId, finalOffset);
                    ExtraInfoUtil.buildMsgOffsetInfo(msgOffsetInfo, isRetry, queueId,
                        result.getMessageQueueOffset());
                } else if ((GetMessageStatus.NO_MATCHED_MESSAGE.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())
                    || GetMessageStatus.MESSAGE_WAS_REMOVING.equals(result.getStatus())
                    || GetMessageStatus.NO_MATCHED_LOGIC_QUEUE.equals(result.getStatus()))
                    && result.getNextBeginOffset() > -1) {
                    // 未拉取到消息，则添加假的消息checkPoint到队列
                    popBufferMergeService.addCkMock(requestHeader.getConsumerGroup(), topic, queueId, finalOffset,
                        requestHeader.getInvisibleTime(), popTime, reviveQid, result.getNextBeginOffset(), brokerController.getBrokerConfig().getBrokerName());
//                this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic,
//                        queueId, getMessageTmpResult.getNextBeginOffset());
                }

                atomicRestNum.set(result.getMaxOffset() - result.getNextBeginOffset() + atomicRestNum.get());
                String brokerName = brokerController.getBrokerConfig().getBrokerName();
                for (SelectMappedBufferResult mapedBuffer : result.getMessageMapedList()) {
                    // We should not recode buffer when popResponseReturnActualRetryTopic is true or topic is not retry topic
                    if (brokerController.getBrokerConfig().isPopResponseReturnActualRetryTopic() || !isRetry) {
                        getMessageResult.addMessage(mapedBuffer);
                    } else {
                        List<MessageExt> messageExtList = MessageDecoder.decodesBatch(mapedBuffer.getByteBuffer(),
                            true, false, true);
                        mapedBuffer.release();
                        for (MessageExt messageExt : messageExtList) {
                            try {
                                String ckInfo = ExtraInfoUtil.buildExtraInfo(finalOffset, popTime, requestHeader.getInvisibleTime(),
                                    reviveQid, messageExt.getTopic(), brokerName, messageExt.getQueueId(), messageExt.getQueueOffset());
                                messageExt.getProperties().putIfAbsent(MessageConst.PROPERTY_POP_CK, ckInfo);

                                // Set retry message topic to origin topic and clear message store size to recode
                                messageExt.setTopic(requestHeader.getTopic());
                                messageExt.setStoreSize(0);

                                byte[] encode = MessageDecoder.encode(messageExt, false);
                                ByteBuffer buffer = ByteBuffer.wrap(encode);
                                SelectMappedBufferResult tmpResult =
                                    new SelectMappedBufferResult(mapedBuffer.getStartOffset(), buffer, encode.length, null);
                                getMessageResult.addMessage(tmpResult);
                            } catch (Exception e) {
                                POP_LOGGER.error("Exception in recode retry message buffer, topic={}", topic, e);
                            }
                        }
                    }
                }
                this.brokerController.getPopInflightMessageCounter().incrementInFlightMessageNum(
                    topic,
                    requestHeader.getConsumerGroup(),
                    queueId,
                    result.getMessageCount()
                );
                return atomicRestNum.get();
            }).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    POP_LOGGER.error("Pop message error, {}", lockKey, throwable);
                }
                queueLockManager.unLock(lockKey);
            });
    }

    private long getPopOffset(String topic, String group, int queueId, int initMode, boolean init, String lockKey,
        boolean checkResetOffset) {

        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, queueId);
        if (offset < 0) {
            if (ConsumeInitMode.MIN == initMode) {
                offset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
            } else {
                // pop last one,then commit offset.
                offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - 1;
                // max & no consumer offset
                if (offset < 0) {
                    offset = 0;
                }
                if (init) {
                    this.brokerController.getConsumerOffsetManager().commitOffset(
                        "getPopOffset", group, topic, queueId, offset);
                }
            }
        }

        if (checkResetOffset) {
            Long resetOffset = resetPopOffset(topic, group, queueId);
            if (resetOffset != null) {
                return resetOffset;
            }
        }

        long bufferOffset = this.popBufferMergeService.getLatestOffset(lockKey);
        if (bufferOffset < 0) {
            return offset;
        } else {
            return Math.max(bufferOffset, offset);
        }
    }

    public final MessageExtBrokerInner buildCkMsg(final PopCheckPoint ck, final int reviveQid) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();

        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ck).getBytes(DataConverter.charset));
        msgInner.setQueueId(reviveQid);
        msgInner.setTags(PopAckConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        // todo 1203提前一秒写入到reviveTopic
        msgInner.setDeliverTimeMs(ck.getReviveTime() - PopAckConstants.ackTimeInterval);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, genCkUniqueId(ck));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }

    /**
     * 1105 构建当前msg对应的ck msg
     * @param requestHeader
     * @param topic
     * @param reviveQid
     * @param queueId
     * @param offset
     * @param getMessageTmpResult
     * @param popTime
     * @param brokerName
     * @return
     */
    private boolean appendCheckPoint(final PopMessageRequestHeader requestHeader,
        final String topic, final int reviveQid, final int queueId, final long offset,
        final GetMessageResult getMessageTmpResult, final long popTime, final String brokerName) {
        // add check point msg to revive log
        // 构建CheckPoint
        final PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        ck.setNum((byte) getMessageTmpResult.getMessageMapedList().size());
        ck.setPopTime(popTime);
        // 消息不可见时间，默认最大60s，这里的不可见时间，仅仅是不可见而已
        // 会使用在下次重试的时候
        ck.setInvisibleTime(requestHeader.getInvisibleTime());
        ck.setStartOffset(offset);
        ck.setCId(requestHeader.getConsumerGroup());
        ck.setTopic(topic);
        ck.setQueueId(queueId);
        ck.setBrokerName(brokerName);
        // add 所有拉取消息与起始偏移量的差值，为了使用inter类型？  拉取的消息唯一不一定是连续的
        // 肯定不一定是连续的，所以要用integer类型来记录一下
        for (Long msgQueueOffset : getMessageTmpResult.getMessageQueueOffset()) {
            ck.addDiff((int) (msgQueueOffset - offset));
        }
        // 将ck放入内存buffer中, 放入之后,会异步检测超时的ck,避免对内存持续占用,将其放入到schedule主题中.
        final boolean addBufferSuc = this.popBufferMergeService.addCk(
            ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset()
        );

        if (addBufferSuc) {
            return true;
        }
        // 放入内存匹配失败（内存匹配未开始），将offset放入内存和磁盘
        return this.popBufferMergeService.addCkJustOffset(
            ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset()
        );
    }

    private Long resetPopOffset(String topic, String group, int queueId) {
        String lockKey = topic + PopAckConstants.SPLIT + group + PopAckConstants.SPLIT + queueId;
        Long resetOffset =
            this.brokerController.getConsumerOffsetManager().queryThenEraseResetOffset(topic, group, queueId);
        if (resetOffset != null) {
            this.brokerController.getConsumerOrderInfoManager().clearBlock(topic, group, queueId);
            this.getPopBufferMergeService().clearOffsetQueue(lockKey);
            this.brokerController.getConsumerOffsetManager()
                .commitOffset("ResetPopOffset", group, topic, queueId, resetOffset);
        }
        return resetOffset;
    }

    private byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group, final String topic,
        final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {

                byteBuffer.put(bb);
                storeTimestamp = bb.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION);
            }
        } finally {
            getMessageResult.release();
        }

        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
            this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

    static class TimedLock {
        private final AtomicBoolean lock;
        private volatile long lockTime;

        public TimedLock() {
            this.lock = new AtomicBoolean(true);
            this.lockTime = System.currentTimeMillis();
        }

        public boolean tryLock() {
            boolean ret = lock.compareAndSet(true, false);
            if (ret) {
                // todo，1125 或者创建时，就应该添加一个时间轮任务
                this.lockTime = System.currentTimeMillis();
                return true;
            } else {
                return false;
            }
        }

        public void unLock() {
            lock.set(true);
        }

        public boolean isLock() {
            return !lock.get();
        }

        public long getLockTime() {
            return lockTime;
        }
    }

    public class QueueLockManager extends ServiceThread {
        // 这个开销挺大的，是不是要控制一下，访问之后超过10s（超时时间）则进行移除。
        private final ConcurrentHashMap<String, TimedLock> expiredLocalCache = new ConcurrentHashMap<>(100000);

        public String buildLockKey(String topic, String consumerGroup, int queueId) {
            return topic + PopAckConstants.SPLIT + consumerGroup + PopAckConstants.SPLIT + queueId;
        }

        public boolean tryLock(String topic, String consumerGroup, int queueId) {
            return tryLock(buildLockKey(topic, consumerGroup, queueId));
        }

        public boolean tryLock(String key) {
            TimedLock timedLock = expiredLocalCache.get(key);

            if (timedLock == null) {
                TimedLock old = expiredLocalCache.putIfAbsent(key, new TimedLock());
                if (old != null) {
                    return false;
                } else {
                    timedLock = expiredLocalCache.get(key);
                }
            }

            if (timedLock == null) {
                return false;
            }

            return timedLock.tryLock();
        }

        /**
         * is not thread safe, may cause duplicate lock
         *
         * @param usedExpireMillis the expired time in millisecond
         * @return total numbers of TimedLock
         */
        public int cleanUnusedLock(final long usedExpireMillis) {
            Iterator<Entry<String, TimedLock>> iterator = expiredLocalCache.entrySet().iterator();

            int total = 0;
            while (iterator.hasNext()) {
                Entry<String, TimedLock> entry = iterator.next();

                if (System.currentTimeMillis() - entry.getValue().getLockTime() > usedExpireMillis) {
                    iterator.remove();
                    POP_LOGGER.info("Remove unused queue lock: {}, {}, {}", entry.getKey(),
                        entry.getValue().getLockTime(),
                        entry.getValue().isLock());
                }

                total++;
            }

            return total;
        }

        public void unLock(String topic, String consumerGroup, int queueId) {
            unLock(buildLockKey(topic, consumerGroup, queueId));
        }

        public void unLock(String key) {
            TimedLock timedLock = expiredLocalCache.get(key);
            if (timedLock != null) {
                timedLock.unLock();
            }
        }

        @Override
        public String getServiceName() {
            if (PopMessageProcessor.this.brokerController.getBrokerConfig().isInBrokerContainer()) {
                return PopMessageProcessor.this.brokerController.getBrokerIdentity().getIdentifier() + QueueLockManager.class.getSimpleName();
            }
            return QueueLockManager.class.getSimpleName();
        }

        /**
         * 默认定时60s查询一次当前超过了60s，且未释放的锁，前行解锁。
         * 那其实还不如直接通过时间轮做个延迟任务
         */
        @Override
        public void run() {
            while (!isStopped()) {
                try {
                    this.waitForRunning(60000);
                    int count = cleanUnusedLock(60000);
                    POP_LOGGER.info("QueueLockSize={}", count);
                } catch (Exception e) {
                    PopMessageProcessor.POP_LOGGER.error("QueueLockManager run error", e);
                }
            }
        }
    }
}
