package com.today.eventbus.annotation;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 描述:
 *
 * @author hz.lei
 * @date 2018年03月04日 下午8:25
 */
public class ListenerCondition {
    private String topic;

    private String groupId;

    private String serializer;

    private String kafkaHostKey;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getSerializer() {
        return serializer;
    }

    public void setSerializer(String serializer) {
        this.serializer = serializer;
    }

    public String getKafkaHostKey() {
        return kafkaHostKey;
    }

    public void setKafkaHostKey(String kafkaHostKey) {
        this.kafkaHostKey = kafkaHostKey;
    }

    public ListenerCondition(Builder builder) {
        this.topic = builder.topic;
        this.groupId = builder.groupId;
        this.serializer = builder.serializer;
        this.kafkaHostKey = builder.kafkaHostKey;
    }

    public ListenerCondition() {
    }

    public static class Builder {

        private String topic;

        private String groupId;

        private String serializer;

        private String kafkaHostKey;


        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder serializer(String serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder kafkaHostKey(String kafkaHostKey) {
            this.kafkaHostKey = kafkaHostKey;
            return this;
        }

        public ListenerCondition build() {
            return new ListenerCondition(this);
        }

    }


}
