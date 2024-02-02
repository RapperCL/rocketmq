package org.apache.rocketmq.store.pop;

public class AckMsgWrapper {
    private AckMsg ackMsg;

    private boolean batch;

    private String mergeKey;

    public String getMergeKey(){
        return ackMsg.getTopic() + ackMsg.getConsumerGroup() + ackMsg.getQueueId() + ackMsg.getStartOffset() + ackMsg.getPopTime() + ackMsg.getBrokerName();
    }
}
