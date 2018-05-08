package com.today.common;

/**
 * 描述: com.today.common
 *
 * @author hz.lei
 * @date 2018年05月07日 下午4:41
 */
public class SysEnvUtil {
    /**
     * 消息总线 定时间隔,默认1s
     */
    private static final String KEY_SOA_EVENTBUS_PERIOD = "soa.eventbus.publish.period";

    public static final String SOA_EVENTBUS_PERIOD = get(KEY_SOA_EVENTBUS_PERIOD, "500");

    public static String get(String key, String defaultValue) {
        String envValue = System.getenv(key.replaceAll("\\.", "_"));

        if (envValue == null) {
            return System.getProperty(key, defaultValue);
        }

        return envValue;
    }
}
