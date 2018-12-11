package com.today.eventbus.common;

import com.today.eventbus.utils.CommonUtil;

/**
 * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
 * @since 2018-11-23 6:05 PM
 */
public class ConsumerContext {
    private final Long key;
    private final String topic;
    private final Long offset;
    private final Integer partition;
    private final Long timestamp;
    private final String timeFormat;
    private final String timestampType;
    private final String eventType;

    public ConsumerContext(Long key, String topic, Long offset, Integer partition, Long timestamp, String timestampType, String eventType) {
        this.key = key;
        this.topic = topic;
        this.offset = offset;
        this.partition = partition;
        this.timestamp = timestamp;
        this.timeFormat = CommonUtil.convertTimestamp(timestamp);
        this.timestampType = timestampType;
        this.eventType = eventType;
    }

    public Long getKey() {
        return key;
    }

    public String getTopic() {
        return topic;
    }

    public Long getOffset() {
        return offset;
    }

    public Integer getPartition() {
        return partition;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public String getTimestampType() {
        return timestampType;
    }

    public String getEventType() {
        return eventType;
    }

    @Override
    public String toString() {
        return "ConsumerContext{" + "key=" + key + ", topic='" + topic + '\'' + ", offset=" + offset + ", partition=" + partition +
                ", timestamp=" + timestamp + ", timeFormat='" + timeFormat + '\'' +
                ", timestampType='" + timestampType + '\'' + ", eventType='" + eventType + '\'' + '}';
    }
}
