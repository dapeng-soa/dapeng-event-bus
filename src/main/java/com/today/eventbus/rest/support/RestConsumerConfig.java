package com.today.eventbus.rest.support;

import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

import java.util.List;

/**
 * 描述: com.today.eventbus.rest.support
 *
 * @author hz.lei
 * @date 2018年05月03日 上午12:45
 */
@Root(name = "consumer")
public class RestConsumerConfig {

    @ElementList(name = "endpoint", type = RestConsumerEndpoint.class, inline = true)
    private List<RestConsumerEndpoint> restConsumerEndpoints;

    public List<RestConsumerEndpoint> getRestConsumerEndpoints() {
        return restConsumerEndpoints;
    }

    public void setRestConsumerEndpoints(List<RestConsumerEndpoint> restConsumerEndpoints) {
        this.restConsumerEndpoints = restConsumerEndpoints;
    }
}
