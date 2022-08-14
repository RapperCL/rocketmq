package org.apache.rocketmq.example.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionListenerBfImpl implements TransactionListener {
    private AtomicInteger transactionIdx = new AtomicInteger(0);

    private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        System.out.println("TransactionListenerBfImpl executeLocalTransaction "+ msg.getTopic()+msg.getTransactionId());
        localTrans.put(msg.getTransactionId(), transactionIdx.getAndIncrement());
        return LocalTransactionState.UNKNOW;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        System.out.println("TransactionListenerBfImpl checkLocalTransaction "+ msg.getTopic()+msg.getTransactionId());
        return LocalTransactionState.UNKNOW;
    }
}
