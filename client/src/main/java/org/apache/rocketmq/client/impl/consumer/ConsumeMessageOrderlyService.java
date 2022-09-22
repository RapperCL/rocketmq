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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
        Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerOrderly messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;

    public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerOrderly messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        String consumeThreadPrefix = null;
        if (consumerGroup.length() > 100) {
            consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").append(consumerGroup.substring(0, 100)).append("_").toString();
        } else {
            consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").append(consumerGroup).append("_").toString();
        }
        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl(consumeThreadPrefix));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    }

    public void start() {
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        // todo 0825 定时加锁续期， broker对于加锁过期时间为60s，
                        // 客户端延迟1s开始加锁，接下来间隔20s，进行续命或更新， 从prcessQueueTable遍历当前clientid对应的mesageQueue
                        // 按理来说，也只应该加 使用了orderService的消费者， 应不应该给topic也附加上这个呢？ 显然是不应该的，其实也可以不用做区分
                        // 全部加也是可以的。因为只有顺序消息才会要求加锁。
                        // 于是会orderService会对该clientid 分配的所有的mq进行加锁，
                        ConsumeMessageOrderlyService.this.lockMQPeriodically();
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate lockMQPeriodically exception", e);
                    }
                }
            }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown(long awaitTerminateMillis) {
        this.stopped = true;
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            this.unlockAllMQ();
        }
    }

    public synchronized void unlockAllMQ() {
        this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeOrderlyContext context = new ConsumeOrderlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeOrderlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case COMMIT:
                        result.setConsumeResult(CMResult.CR_COMMIT);
                        break;
                    case ROLLBACK:
                        result.setConsumeResult(CMResult.CR_ROLLBACK);
                        break;
                    case SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                RemotingHelper.exceptionSimpleDesc(e),
                ConsumeMessageOrderlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

        result.setAutoCommit(context.isAutoCommit());
        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispathToConsume) {
        if (dispathToConsume) {
            ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
            this.consumeExecutor.submit(consumeRequest);
        }
    }

    public synchronized void lockMQPeriodically() {
        if (!this.stopped) {
            this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
        }
    }

    public void tryLockLaterAndReconsume(final MessageQueue mq, final ProcessQueue processQueue,
        final long delayMills) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                boolean lockOK = ConsumeMessageOrderlyService.this.lockOneMQ(mq);
                if (lockOK) {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 10);
                } else {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 3000);
                }
            }
        }, delayMills, TimeUnit.MILLISECONDS);
    }

    public synchronized boolean lockOneMQ(final MessageQueue mq) {
        if (!this.stopped) {
            return this.defaultMQPushConsumerImpl.getRebalanceImpl().lock(mq);
        }

        return false;
    }

    private void submitConsumeRequestLater(
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final long suspendTimeMillis
    ) {
        long timeMillis = suspendTimeMillis;
        if (timeMillis == -1) {
            timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
        }

        if (timeMillis < 10) {
            timeMillis = 10;
        } else if (timeMillis > 30000) {
            timeMillis = 30000;
        }

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageOrderlyService.this.submitConsumeRequest(null, processQueue, messageQueue, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }

    public boolean processConsumeResult(
        final List<MessageExt> msgs,
        final ConsumeOrderlyStatus status,
        final ConsumeOrderlyContext context,
        final ConsumeRequest consumeRequest
    ) {
        boolean continueConsume = true;
        long commitOffset = -1L;
        // 自动提交， 自动提交是不是 定时提交呢？
        if (context.isAutoCommit()) {
            switch (status) {
                case COMMIT:
                case ROLLBACK:
                    log.warn("the message queue consume result is illegal, we think you want to ack these message {}",
                        consumeRequest.getMessageQueue());
                case SUCCESS:
                    //
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                    // 只会存在消费成功和过一会儿消费， 因为rollback和commit只面向 事务消息了
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    // 未超过最大重试次数，则继续进行重试
                    if (checkReconsumeTimes(msgs)) {
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                        // 此时的重试是默认1s之后，延迟消费。
                        this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue(),
                            context.getSuspendCurrentQueueTimeMillis());
                        // 如果是重试消费的话，那么则会中断当前的消费过程
                        continueConsume = false;
                        //
                    } else {
                        commitOffset = consumeRequest.getProcessQueue().commit();
                    }
                    break;
                default:
                    break;
            }
        } else {
            switch (status) {
                case SUCCESS:
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                case COMMIT:
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    break;
                case ROLLBACK:
                    consumeRequest.getProcessQueue().rollback();
                    this.submitConsumeRequestLater(
                        consumeRequest.getProcessQueue(),
                        consumeRequest.getMessageQueue(),
                        context.getSuspendCurrentQueueTimeMillis());
                    continueConsume = false;
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                        this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue(),
                            context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    }
                    break;
                default:
                    break;
            }
        }

        // 更新offsetTable的位移
        if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), commitOffset, false);
        }

        return continueConsume;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: Integer.MAX_VALUE
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return Integer.MAX_VALUE;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    private boolean checkReconsumeTimes(List<MessageExt> msgs) {
        boolean suspend = false;
        if (msgs != null && !msgs.isEmpty()) {
            for (MessageExt msg : msgs) {
                // 重试次数 >= 最大重试次数时 默认最大的重试次数为16次
                if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
                    MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));
                    // 如果发送到重试主题失败，那么会再次尝试本地消费一次。  // 需不需要做个控制，如果一直发送失败，那么会导致本地的消息一直重试
                    if (!sendMessageBack(msg)) {
                        suspend = true;
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    }
                    // 小于重试次数时
                } else {
                    suspend = true;
                    // 重试的消费次数+1
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                }
            }
        }
        return suspend;
    }

    public boolean sendMessageBack(final MessageExt msg) {
        try {
            // max reconsume times exceeded then send to dead letter queue.
            // max reconsume times exceeded then send to dead letter queue.  %RETRY% + 消费者组
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            // todo 0828 将对应真实的主题保存到其属性中
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes()));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());
            // 将消息进行发送
            this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getDefaultMQProducer().send(newMsg);
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    public void resetNamespace(final List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    class ConsumeRequest implements Runnable {
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.warn("run, the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                return;
            }

            // 获取messageQueue对应的锁
            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
            synchronized (objLock) {
                //todo 0827  广播或 当前处理队列锁住了切换未过期（ client端也有过期，30s，因为是20s更新一次，所以最好大于20s）
                if (MessageModel.BROADCASTING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                    || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {
                    final long beginTime = System.currentTimeMillis();
                    for (boolean continueConsume = true; continueConsume; ) {
                        if (this.processQueue.isDropped()) {
                            log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                            break;
                        }
                        // 按理来说不会走到这里,，初次不会，后续可能会？
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && !this.processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", this.messageQueue);
                            // 是集群模式，但是没有锁住了，就只能尝试锁住并重新进行消费了
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }
                        // 锁过期了
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && this.processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        long interval = System.currentTimeMillis() - beginTime;
                        // todo 0827 需要注意这里做了限制： 消息的最大连续消费时间为60s，那其实就和broker端对应，防止了重复消费的情况

                        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                            break;
                        }

                        final int consumeBatchSize =
                            ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
                        // 顺序消息最大的批量消费个数 = 1
                        // 从msgTreeMap中获取消息并进行消费
                        List<MessageExt> msgs = this.processQueue.takeMessages(consumeBatchSize);
                        defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());
                        if (!msgs.isEmpty()) {
                            final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);

                            ConsumeOrderlyStatus status = null;

                            ConsumeMessageContext consumeMessageContext = null;
                            // 存在回调函数，则先进行回调，在重要操作前后开放监听，便于前后扩展
                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext = new ConsumeMessageContext();
                                consumeMessageContext
                                    .setConsumerGroup(ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumerGroup());
                                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                                consumeMessageContext.setMq(messageQueue);
                                consumeMessageContext.setMsgList(msgs);
                                consumeMessageContext.setSuccess(false);
                                // init the consume context type
                                consumeMessageContext.setProps(new HashMap<String, String>());
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                            }

                            long beginTimestamp = System.currentTimeMillis();
                            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                            boolean hasException = false;
                            try {
                                // 消费锁锁住 ，单个队列锁
                                this.processQueue.getConsumeLock().lock();
                                if (this.processQueue.isDropped()) {
                                    log.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                                        this.messageQueue);
                                    break;
                                }
                                // 调用接口实现类，也就是我们实现的类，实现消息消费
                                // success  rollback和 commit 仅用于事务消费， 还有暂停片刻消费
                                status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                            } catch (Throwable e) {
                                log.warn(String.format("consumeMessage exception: %s Group: %s Msgs: %s MQ: %s",
                                    RemotingHelper.exceptionSimpleDesc(e),
                                    ConsumeMessageOrderlyService.this.consumerGroup,
                                    msgs,
                                    messageQueue), e);
                                hasException = true;
                            } finally {
                                this.processQueue.getConsumeLock().unlock();
                            }

                            if (null == status
                                || ConsumeOrderlyStatus.ROLLBACK == status
                                || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                                    ConsumeMessageOrderlyService.this.consumerGroup,
                                    msgs,
                                    messageQueue);
                            }

                            long consumeRT = System.currentTimeMillis() - beginTimestamp;
                            if (null == status) {
                                if (hasException) {
                                    returnType = ConsumeReturnType.EXCEPTION;
                                } else {
                                    returnType = ConsumeReturnType.RETURNNULL;
                                }
                            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                                returnType = ConsumeReturnType.TIME_OUT;
                            } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                returnType = ConsumeReturnType.FAILED;
                            } else if (ConsumeOrderlyStatus.SUCCESS == status) {
                                returnType = ConsumeReturnType.SUCCESS;
                            }


                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                            }

                            // 返回为空时，默认过一会儿进行消费？
                            if (null == status) {
                                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                            }
                            //todo 0828 处理结果之前的回调
                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.setStatus(status.toString());
                                consumeMessageContext
                                    .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                            }

                            ConsumeMessageOrderlyService.this.getConsumerStatsManager()
                                .incConsumeRT(ConsumeMessageOrderlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);
                            //todo 0828  处理消费结果， 根据消费结果，选择重新消费或者更新processQueue的位移
                            continueConsume = ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status, context, this);
                        } else {
                            continueConsume = false;
                        }
                    }
                } else {
                    if (this.processQueue.isDropped()) {
                        log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                        return;
                    }

                    ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
                }
            }
        }

    }

}
