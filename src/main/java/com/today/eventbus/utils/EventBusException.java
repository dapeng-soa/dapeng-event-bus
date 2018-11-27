package com.today.eventbus.utils;

/**
 * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
 * @since 2018-11-27 12:38 PM
 */
public class EventBusException extends RuntimeException {

    public EventBusException(String message, Throwable cause) {
        super(message, cause);
    }

    public EventBusException(String message) {
        super(message);
    }
}
