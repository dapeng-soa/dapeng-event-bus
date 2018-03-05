package com.today.eventbus.spring;


/**
 * 描述:
 *
 * @author hz.lei
 * @date 2018年03月04日 下午8:25
 */
public class KafkaListenerInfo {
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

}
