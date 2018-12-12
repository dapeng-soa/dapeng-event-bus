package com.today.eventbus.agent.support.http;

/**
 * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
 * @since 2018-12-12 3:05 PM
 */
public enum StrategyType {

    OK_HTTP(1),
    HTTP_CLIENT(2),
    ASYNC_HTTP(3);

    private int type;

    StrategyType(int type) {
        this.type = type;
    }

    public static StrategyType getType(int type) {
        switch (type) {
            case 1:
                return OK_HTTP;
            case 2:
                return HTTP_CLIENT;
            case 3:
                return ASYNC_HTTP;
            default:
                return OK_HTTP;
        }
    }

    public static HttpStrategy buildStrategy(StrategyType type) {
        switch (type) {
            case OK_HTTP:
                return new OkHttpStrategy();
            case HTTP_CLIENT:
                return new HttpClientStrategy();
            case ASYNC_HTTP:
                return new AsyncHttpStrategy();
            default:
                return new OkHttpStrategy();
        }
    }

}
