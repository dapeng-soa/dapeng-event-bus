package com.today.eventbus.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
 * @since 2018-11-27 1:47 PM
 */
public class CommonUtil {

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss SSS");

    public static String convertTimestamp(long timeStamp) {
        return LocalDateTime.now(ZoneId.of("Asia/Shanghai")).format(formatter);
    }
}
