package com.today.eventbus.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 描述: com.today.eventbus.common
 *
 * @author hz.lei
 * @since 2018年05月07日 下午4:41
 */
public class SysEnvUtil {


    /**
     * 消息总线 定时间隔,默认1s
     */
    private static final String KEY_SOA_EVENTBUS_PERIOD = "soa.eventbus.publish.period";
    /**
     * 当前环境容器IP
     */
    private static final String KEY_HOST_IP = "host_ip";


    public static final String SOA_EVENTBUS_PERIOD = get(KEY_SOA_EVENTBUS_PERIOD, "300");

    public static final String HOST_IP = get(KEY_HOST_IP, localIp());


    public static String get(String key, String defaultValue) {
        String envValue = System.getenv(key.replaceAll("\\.", "_"));

        if (envValue == null) {
            return System.getProperty(key, defaultValue);
        }

        return envValue;
    }

    public static String localIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "-UnknownIp";
        }
    }
}
