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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.PopMetricsManager;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BatchAck;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;

public class AckMessageProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    private final String reviveTopic;
    private final PopReviveService[] popReviveServices;

    public AckMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.reviveTopic = PopAckConstants.buildClusterReviveTopic(this.brokerController.getBrokerConfig().getBrokerClusterName());
        this.popReviveServices = new PopReviveService[this.brokerController.getBrokerConfig().getReviveQueueNum()];
        for (int i = 0; i < this.brokerController.getBrokerConfig().getReviveQueueNum(); i++) {
            this.popReviveServices[i] = new PopReviveService(brokerController, reviveTopic, i);
            this.popReviveServices[i].setShouldRunPopRevive(brokerController.getBrokerConfig().getBrokerId() == 0);
        }
    }

    public PopReviveService[] getPopReviveServices() {
        return popReviveServices;
    }

    public void startPopReviveService() {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.start();
        }
    }

    public void shutdownPopReviveService() {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.shutdown();
        }
    }

    public void setPopReviveServiceStatus(boolean shouldStart) {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.setShouldRunPopRevive(shouldStart);
        }
    }

    public boolean isPopReviveServiceRunning() {
        for (PopReviveService popReviveService : popReviveServices) {
            if (popReviveService.isShouldRunPopRevive()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand processRequest(final Channel channel, RemotingCommand request,
                                           boolean brokerAllowSuspend) throws RemotingCommandException {
        AckMessageRequestHeader requestHeader;

        final RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setOpaque(request.getOpaque());
        // 消费者发回ack消息，更新消息消费位移？ checkPoint与消费位移的联系
        if (request.getCode() == RequestCode.ACK_MESSAGE) {
            requestHeader = (AckMessageRequestHeader) request.decodeCommandCustomHeader(AckMessageRequestHeader.class);

            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
            if (null == topicConfig) {
                POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
                response.setCode(ResponseCode.TOPIC_NOT_EXIST);
                response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
                return response;
            }

            if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums() || requestHeader.getQueueId() < 0) {
                String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                        requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
                POP_LOGGER.warn(errorInfo);
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(errorInfo);
                return response;
            }

            long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
            long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
            if (requestHeader.getOffset() < minOffset || requestHeader.getOffset() > maxOffset) {
                String errorInfo = String.format("offset is illegal, key:%s@%d, commit:%d, store:%d~%d",
                        requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getOffset(), minOffset, maxOffset);
                POP_LOGGER.warn(errorInfo);
                response.setCode(ResponseCode.NO_MESSAGE);
                response.setRemark(errorInfo);
                return response;
            }

            appendAck(requestHeader, null, response, channel, null);
        } else if (request.getCode() == RequestCode.BATCH_ACK_MESSAGE) {
            BatchAckMessageRequestBody reqBody = null;
            if (request.getBody() != null) {
                reqBody = BatchAckMessageRequestBody.decode(request.getBody(), BatchAckMessageRequestBody.class);
            }
            if (reqBody == null || reqBody.getAcks() == null || reqBody.getAcks().isEmpty()) {
                response.setCode(ResponseCode.NO_MESSAGE);
                return response;
            }
            for (BatchAck bAck : reqBody.getAcks()) {
                appendAck(null, bAck, response, channel, reqBody.getBrokerName());
            }
        } else {
            POP_LOGGER.error("AckMessageProcessor failed to process RequestCode: {}, consumer: {} ", request.getCode(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark(String.format("AckMessageProcessor failed to process RequestCode: %d", request.getCode()));
            return response;
        }
        return response;
    }

    private void appendAck(final AckMessageRequestHeader requestHeader, final BatchAck batchAck, final RemotingCommand response, final Channel channel, String brokerName) {
        String[] extraInfo;
        String consumeGroup, topic;
        // 消费队列id， 重试消费队列id
        int qId, rqId;
        // 起始位移， ack位移
        long startOffset, ackOffset;
        // pop拉取时间，不可见时间
        long popTime, invisibleTime;
        AckMsg ackMsg;
        int ackCount = 0;
        if (batchAck == null) {
            // single ack
            extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());
            brokerName = ExtraInfoUtil.getBrokerName(extraInfo);
            consumeGroup = requestHeader.getConsumerGroup();
            topic = requestHeader.getTopic();
            qId = requestHeader.getQueueId();
            rqId = ExtraInfoUtil.getReviveQid(extraInfo);
            startOffset = ExtraInfoUtil.getCkQueueOffset(extraInfo);
            ackOffset = requestHeader.getOffset();
            popTime = ExtraInfoUtil.getPopTime(extraInfo);
            invisibleTime = ExtraInfoUtil.getInvisibleTime(extraInfo);

            if (rqId == KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
                // order
                String lockKey = topic + PopAckConstants.SPLIT + consumeGroup + PopAckConstants.SPLIT + qId;
                long oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(consumeGroup, topic, qId);
                if (ackOffset < oldOffset) {
                    return;
                }
                while (!this.brokerController.getPopMessageProcessor().getQueueLockManager().tryLock(lockKey)) {
                }
                try {
                    oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(consumeGroup, topic, qId);
                    if (ackOffset < oldOffset) {
                        return;
                    }
                    long nextOffset = brokerController.getConsumerOrderInfoManager().commitAndNext(
                            topic, consumeGroup,
                            qId, ackOffset,
                            popTime);
                    if (nextOffset > -1) {
                        if (!this.brokerController.getConsumerOffsetManager().hasOffsetReset(
                                topic, consumeGroup, qId)) {
                            this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(),
                                    consumeGroup, topic, qId, nextOffset);
                        }
                        if (!this.brokerController.getConsumerOrderInfoManager().checkBlock(null, topic,
                                consumeGroup, qId, invisibleTime)) {
                            this.brokerController.getPopMessageProcessor().notifyMessageArriving(
                                    topic, consumeGroup, qId);
                        }
                    } else if (nextOffset == -1) {
                        String errorInfo = String.format("offset is illegal, key:%s, old:%d, commit:%d, next:%d, %s",
                                lockKey, oldOffset, ackOffset, nextOffset, channel.remoteAddress());
                        POP_LOGGER.warn(errorInfo);
                        response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                        response.setRemark(errorInfo);
                        return;
                    }
                } finally {
                    this.brokerController.getPopMessageProcessor().getQueueLockManager().unLock(lockKey);
                }
                brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(topic, consumeGroup, popTime, qId, ackCount);
                return;
            }

            ackMsg = new AckMsg();
            ackCount = 1;
        } else {
            // batch ack
            consumeGroup = batchAck.getConsumerGroup();
            topic = ExtraInfoUtil.getRealTopic(batchAck.getTopic(), batchAck.getConsumerGroup(), ExtraInfoUtil.RETRY_TOPIC.equals(batchAck.getRetry()));
            qId = batchAck.getQueueId();
            rqId = batchAck.getReviveQueueId();
            startOffset = batchAck.getStartOffset();
            ackOffset = -1;
            popTime = batchAck.getPopTime();
            invisibleTime = batchAck.getInvisibleTime();

            long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, qId);
            long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, qId);
            // maxOffset 不存在为-1的情况，ack消息，那么一定是有值的
            if (minOffset == -1 || maxOffset == 0) {
                POP_LOGGER.error("Illegal topic or queue found when batch ack {}", batchAck);
                return;
            }

            BatchAckMsg batchAckMsg = new BatchAckMsg();
            for (int i = 0; batchAck.getBitSet() != null && i < batchAck.getBitSet().length(); i++) {
                //
                if (!batchAck.getBitSet().get(i)) {
                    continue;
                }// todo 1125 为什么offset一定是连续的？ 如果是这样的话，那么这里的batchAckMsg，同样可以压缩内存，
                //todo 1125 没必要采用long集合存储， 就可以压缩内存了
                // 差异肯定是小于int的， 如果差异大的话，bitset就占很大内存了。
                long offset = startOffset + i;
                if (offset < minOffset || offset > maxOffset) {
                    continue;
                }
                // 或者batchAckMsg 也不需要解析出真正的offset。
                // 解析出来之后，也没有干啥
                batchAckMsg.getAckOffsetLists().add(9);
                batchAckMsg.getAckOffsetList().add(offset);
            }
            if (batchAckMsg.getAckOffsetList().isEmpty()) {
                return;
            }

            ackMsg = batchAckMsg;
            ackCount = batchAckMsg.getAckOffsetList().size();
        }

        this.brokerController.getBrokerStatsManager().incBrokerAckNums(ackCount);
        this.brokerController.getBrokerStatsManager().incGroupAckNums(consumeGroup, topic, ackCount);

        ackMsg.setConsumerGroup(consumeGroup);
        ackMsg.setTopic(topic);
        ackMsg.setQueueId(qId);
        // todo ackMsg就是会包含startOffset ，为什么batchAckMsg需要使用List<long>类型记录
        ackMsg.setStartOffset(startOffset);
        ackMsg.setAckOffset(ackOffset);
        ackMsg.setPopTime(popTime);
        ackMsg.setBrokerName(brokerName);
        // 先尝试放入内存匹配，可能失败，失败代表内存匹配未开始,或者ck已经写入到磁盘，buffer中已经被
        // 删除了
        if (this.brokerController.getPopMessageProcessor().getPopBufferMergeService().addAk(rqId, ackMsg)) {
            brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(topic, consumeGroup, popTime, qId, ackCount);
            return;
        }
        /**
         * 内存中匹配失败，则构造ack消息，将ack消息存入磁盘
         */
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.charset));
        msgInner.setQueueId(rqId);
        if (ackMsg instanceof BatchAckMsg) {
            msgInner.setTags(PopAckConstants.BATCH_ACK_TAG);
            msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genBatchAckUniqueId((BatchAckMsg) ackMsg));
        } else {
            msgInner.setTags(PopAckConstants.ACK_TAG);
            msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genAckUniqueId(ackMsg));
        }
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        // ack消息的下次投递时间，其实就是不可见时间，和对应ck消息的时间一致。
        msgInner.setDeliverTimeMs(popTime + invisibleTime);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genAckUniqueId(ackMsg));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = this.brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
                && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
                && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
                && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("put ack msg error:" + putMessageResult);
        }
        System.out.printf("put ack to store %s", ackMsg);
        PopMetricsManager.incPopReviveAckPutCount(ackMsg, putMessageResult.getPutMessageStatus());
        brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(topic, consumeGroup, popTime, qId, ackCount);
    }
}
