package com.today.eventbus.utils;

/**
 * 描述:
 *
 * @author hz.lei
 * @date 2018年03月03日 下午4:54
 */
public class MsgInfo<T> {
    private String eventType;
    private T event;


    public MsgInfo(String eventType, T event) {
        this.eventType = eventType;
        this.event = event;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public T getEvent() {
        return event;
    }

    public void setEvent(T event) {
        this.event = event;
    }
}
