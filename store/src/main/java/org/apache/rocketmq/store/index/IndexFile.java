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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    private final int hashSlotNum;
    private final int indexNum;
    private final MappedFile mappedFile;
    private final MappedByteBuffer mappedByteBuffer;
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        // todo 利用hash槽来存放index数据，hashMap的结构， 规定存放的index数目不超过2000万个，但不代表每个hashslot只能存放4个index
        // 因为做这样的限制很鸡肋，分布肯定是不均衡的，如果做了限制会导致严重后果： 有的slot为空，有的需要存的index超过4个
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            int keyHash = indexKeyHashMethod(key);
            // 计算哈希槽
            int slotPos = keyHash % this.hashSlotNum;
            // 绝对哈希槽 = indexHeader + 哈希槽位置 * 每个哈希槽的大小
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                 // 取出对应的槽值 indexNum
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }
                //计算key的绝对位移（消息物理偏移量） hashMap的思路实现。 实际的内存存储： 先写入key，然后写入value，
                // 物理上，key和value都是写到了内存中； 逻辑上有 key和value。
                // 计算需要写入的开始下标= 20 + hashslotNum 500 0000 * hashSlotSize4 + （已写入的）indexCount * indexSize20 接着后面进行写入

                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;
                // 绝对位移： key的值， 物理偏移 占4位， 时间差占8位   槽值占4位
                // 从absIndexPos这个下标处，接着写入  hashcode
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                // 往后移4位，写入消息在commitLog中的物理偏移量
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                // 往后移动8位，写入消息的时间差
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // 往后移动4位，接着写入前一个hash的indexNum = 20 + x * 4
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                // 更新对应hashslot的indexnum
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                // 只有一条消息时， 设置起始位移和时间戳
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }
                // 每个hashslot的初始值都是invalidIndex， 代表占用了新的槽位，于是个数自增
                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }
                // 消息数
                this.indexHeader.incIndexCount();
                // 更新结束物理偏移量
                this.indexHeader.setEndPhyOffset(phyOffset);
                // 更新结束存储时间戳
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }
    // 根据对应的key查找 指定数目的消息
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            // 根据key找到对应的hashslot
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                //  获取对应的hashvalue
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // 槽位没有对应的值｜｜ 大于当前的总数｜｜ 只有一个index时，不做处理
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ){
                        // 达到需要获取的数目时
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }
                        // 读取消息 20字节： hash，物理偏移量，时间差，下一个num
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);
                        // 要求符合
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }
                        // 遍历到前一个消息，问题是哦，最大只能有4个index，那么此时的maxNum就是<=4
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                this.mappedFile.release();
            }
        }
    }
}
