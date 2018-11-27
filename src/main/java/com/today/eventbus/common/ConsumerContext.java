package com.today.eventbus.common;

import com.today.eventbus.utils.CommonUtil;

/**
 * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
 * @since 2018-11-23 6:05 PM
 */
public class ConsumerContext<K> {
    private final K key;
    private final String topic;
    private final long offset;
    private final int partition;
    private final long timestamp;
    private final String timeFormat;
    private final String timestampType;

    public ConsumerContext(K key, String topic, long offset, int partition, long timestamp, String timestampType) {
        this.key = key;
        this.topic = topic;
        this.offset = offset;
        this.partition = partition;
        this.timestamp = timestamp;
        this.timeFormat = CommonUtil.convertTimestamp(timestamp);
        this.timestampType = timestampType;
    }

    public K getKey() {
        return key;
    }

    public String getTopic() {
        return topic;
    }

    public long getOffset() {
        return offset;
    }

    public int getPartition() {
        return partition;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public String getTimestampType() {
        return timestampType;
    }

    @Override
    public String toString() {
        return "Current ConsumerContext:[" + "key=" + key + ", topic='" + topic + '\'' + ", offset=" + offset + ", partition=" + partition +
                ", timestamp=" + timestamp + ", timeFormat='" + timeFormat + '\'' + ", timestampType='" + timestampType + '\'' + ']';
    }
}
