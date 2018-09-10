package com.today.eventbus.utils;

/**
 * 描述:
 *
 * @author hz.lei
 * @since 2018年03月04日 下午9:13
 */
public interface Constant {

    String DEFAULT_CONSUMER_HOST_KEY = "kafka.consumer.host";

    String DEFAULT_EVENT_DATA_TABLE_NAME = "dp_common_event";

    String DEFAULT_EVENT_LOCK_TABLE_NAME = "dp_event_lock";

    String KAFKA_LISTENER_REGISTRAR_BEAN_NAME = "kafkaListenerRegistrar";
    /**
     * ms
     */
    int DEFAULT_SESSION_TIMEOUT = 10000;

    /**
     * per poll max message size
     */
    String MAX_POLL_SIZE = "50";

    /**
     * isolation
     */
    String ISOLATION_LEVEL = "read_committed";
}
