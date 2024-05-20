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
package org.apache.rocketmq.store;


import java.util.concurrent.LinkedBlockingQueue;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.CommitLog.GroupCommitRequest;

public class FlushDiskWatcher extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final LinkedBlockingQueue<GroupCommitRequest> commitRequests = new LinkedBlockingQueue<>();

    @Override
    public String getServiceName() {
        return FlushDiskWatcher.class.getSimpleName();
    }

    @Override
    // 异步设计 需要额外搞个监听的吗？  额外增加了存储，而且睡眠时间没有分段睡眠，或者说有唤醒机制。
    // 这里不应该直接sleep，应该换用waiter？ 这里的目的应该是及时返回超时的吗？ 那应该按照优先级队列，
    // 以deadline排序， 或者应该直接用时间轮替换掉，每个flushDisk都通过时间轮挂载一个延迟任务
    public void run() {
        while (!isStopped()) {
            GroupCommitRequest request = null;
            try {
                request = commitRequests.take();
            } catch (InterruptedException e) {
                log.warn("take flush disk commit request, but interrupted, this may caused by shutdown");
                continue;
            }
            while (!request.future().isDone()) {
                long now = System.nanoTime();
                if (now - request.getDeadLine() >= 0) {
                    request.wakeupCustomer(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    break;
                }
                // To avoid frequent thread switching, replace future.get with sleep here,
                long sleepTime = (request.getDeadLine() - now) / 1_000_000;
                sleepTime = Math.min(10, sleepTime);
                if (sleepTime == 0) {
                    request.wakeupCustomer(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    break;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    log.warn(
                            "An exception occurred while waiting for flushing disk to complete. this may caused by shutdown");
                    break;
                }
            }
        }
    }

    public void add(GroupCommitRequest request) {
        commitRequests.add(request);
    }

    public int queueSize() {
        return commitRequests.size();
    }
}
