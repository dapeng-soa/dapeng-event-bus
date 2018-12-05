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
    private static final String KEY_HOST_IP = "host.ip";

    /**
     * 当前配置消费者 消息回溯 seek 重新消费 的地址路径
     */
    private static final String KEY_MSG_BACK_TRACKING = "msg.back.tracking.path";


    public static final String SOA_EVENTBUS_PERIOD = get(KEY_SOA_EVENTBUS_PERIOD, "300");

    public static final String HOST_IP = get(KEY_HOST_IP, localIp());


    public static final String MSG_BACK_TRACKING = get(KEY_MSG_BACK_TRACKING, "application.conf");


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
