package com.today.eventbus.config;

import com.today.eventbus.common.SysEnvUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * 描述: kafka producer和consumer properties 默认配置
 *
 * @author hz.lei
 * @since 2018年02月26日 下午2:45
 */
public abstract class KafkaConfigBuilder {

    protected final Properties properties = new Properties();

    public static ProducerConfiguration defaultProducer() {
        return defaultProducer(new Properties());
    }

    public static ConsumerConfiguration defaultConsumer() {
        return defaultConsumer(new Properties());
    }

    public static ProducerConfiguration defaultProducer(final Properties properties) {
        final ProducerConfiguration builder = new ProducerConfiguration();
        builder.withKeySerializer(StringSerializer.class);
        builder.withValueSerializer(StringSerializer.class);
        /**
         * acks 指定了“all”将会阻塞消息，这种设置性能最低，但是是最可靠的
         */
        builder.properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // retries 如果配置为0，不会有重复发送消息的问题
        builder.properties.put(ProducerConfig.RETRIES_CONFIG, 1);
        // batch.size 缓存每个分区未发送消息,缓冲区大小
        builder.properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // linger.ms 批量等待时间
        builder.properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // buffer.memory
        builder.properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        fill(properties, builder.properties);
        return builder;
    }


    public static ConsumerConfiguration defaultConsumer(final Properties properties) {
        final ConsumerConfiguration builder = new ConsumerConfiguration();
        builder.properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, System.getProperty("max.poll.interval.ms", "60000"));
        builder.withKeyDeserializer(StringDeserializer.class);
        builder.withValueDeserializer(StringDeserializer.class);
        builder.withClientId("Consumer-" + SysEnvUtil.HOST_IP + "-" + UUID.randomUUID().toString());
        fill(properties, builder.properties);
        return builder;
    }

    public KafkaConfigBuilder withProperty(final String propertyName, final String propertyValue) {
        if (propertyValue != null) {
            properties.put(propertyName, propertyValue);
        }
        return this;
    }

    public KafkaConfigBuilder withSystemProperty(final String propertyName, final String systemPropertyName) {
        final String propertyValue = System.getProperty(systemPropertyName);
        if (propertyValue != null) {
            properties.put(propertyName, propertyValue);
        }
        return this;
    }

    public Properties build() {
        validate();
        return properties;
    }

    abstract void validate();

    public Map<String, Object> asMap() {
        final Map<String, Object> result = new HashMap<String, Object>();
        properties.keySet().stream().forEach(key -> result.put((String) key, properties.get(key)));
        return result;
    }

    public static class ConsumerConfiguration extends KafkaConfigBuilder {
        public ConsumerConfiguration withKeyDeserializer(final Class<? extends Deserializer<?>> clazz) {
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, clazz.getName());
            return this;
        }

        public ConsumerConfiguration withValueDeserializer(final Class<? extends Deserializer<?>> clazz) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, clazz.getName());
            return this;
        }

        public ConsumerConfiguration bootstrapServers(final String bootstrapServers) {
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            return this;
        }

        public ConsumerConfiguration group(final String group) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
            return this;
        }

        public ConsumerConfiguration withOffsetCommitted(boolean flag) {
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, flag);
            return this;
        }

        public ConsumerConfiguration withOffsetCommittedInterval(String ms) {
            properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, ms);
            return this;
        }

        public ConsumerConfiguration withIsolation(String level) {
            properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, level);
            return this;
        }

        public ConsumerConfiguration withSessionTimeOut(long sessionTimeOut) {
            properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeOut);
            return this;
        }

        public ConsumerConfiguration withClientId(String clientId) {
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            return this;
        }

        public ConsumerConfiguration excludeInternalTopic(boolean isExcluedTopics) {
            properties.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, isExcluedTopics);
            return this;
        }

        public ConsumerConfiguration maxPollSize(String pollSize) {
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, pollSize);
            return this;
        }

        public ConsumerConfiguration withHeartBeatInterval(long interval) {
            properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, interval);
            return this;
        }


        @Override
        void validate() {
            notNull(properties.get(ConsumerConfig.GROUP_ID_CONFIG), "Group must be set.");
            notNull(properties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), "Bootstrap servers must be set.");
        }
    }

    public static class ProducerConfiguration extends KafkaConfigBuilder {

        public ProducerConfiguration withKeySerializer(final Class<? extends Serializer<?>> clazz) {
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, clazz.getName());
            return this;
        }

        public ProducerConfiguration withValueSerializer(final Class<? extends Serializer<?>> clazz) {
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, clazz.getName());
            return this;
        }

        public ProducerConfiguration bootstrapServers(final String bootstrapServers) {
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            return this;
        }

        /**
         * 若要开启事务支持，除了配置transId外，还要配置生产者开启幂等
         *
         * @param transactionId
         * @return
         */
        public ProducerConfiguration withTransactions(final String transactionId) {
            properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
            return this;
        }

        /**
         * 当设置为‘true’，生产者将确保每个消息正好一次复制写入到stream。
         * 如果‘false’，由于broker故障，生产者重试。即，可以在流中写入重试的消息。此设置默认是‘false’。
         * 请注意，启用幂等式需要将max.in.flight.requests.per.connection设置为1或者等于5(默认)，
         * ，重试次数不能为零。另外acks必须设置为“all”。如果这些值保持默认值，我们将覆盖默认值。
         * 如果这些值设置为与幂等生成器不兼容的值，则将抛出一个ConfigException异常。
         *
         * @param flag
         * @return
         */
        public ProducerConfiguration withIdempotence(boolean flag) {
            properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, flag);
            return this;
        }


        @Override
        void validate() {
            notNull(properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), "Bootstrap servers must be set.");
        }


    }

    private static void fill(final Properties source, final Properties target) {
        if (source != null && !source.isEmpty() && target != null) {
            //source.forEach((key, value) -> target.put(key, value));
            source.forEach(target::put);
        }

    }

    /**
     * not null
     */
    public static void notNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }
}
