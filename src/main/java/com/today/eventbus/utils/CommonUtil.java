package com.today.eventbus.utils;

import com.today.eventbus.common.SysEnvUtil;
import com.today.eventbus.config.ConfigLoader;
import com.today.eventbus.config.ResumeConfig;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
 * @since 2018-11-27 1:47 PM
 */
public class CommonUtil {

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss SSS");

    private static Map<String, ResumeConfig> resumeConfigList = new HashMap<>();

    private static volatile boolean loadFlag = false;

    public static String convertTimestamp(Long timeStamp) {
        if (timeStamp == null) {
            return "";
        }
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timeStamp), ZoneId.of("Asia/Shanghai")).format(formatter);
    }

    public static String decodeUnicode(String theString) {
        char aChar;
        int len = theString.length();
        StringBuffer outBuffer = new StringBuffer(len);
        for (int x = 0; x < len; ) {
            aChar = theString.charAt(x++);
            if (aChar == '\\') {
                aChar = theString.charAt(x++);
                if (aChar == 'u') {
                    // Read the xxxx
                    int value = 0;
                    for (int i = 0; i < 4; i++) {
                        aChar = theString.charAt(x++);
                        switch (aChar) {
                            case '0':
                            case '1':
                            case '2':
                            case '3':
                            case '4':
                            case '5':
                            case '6':
                            case '7':
                            case '8':
                            case '9':
                                value = (value << 4) + aChar - '0';
                                break;
                            case 'a':
                            case 'b':
                            case 'c':
                            case 'd':
                            case 'e':
                            case 'f':
                                value = (value << 4) + 10 + aChar - 'a';
                                break;
                            case 'A':
                            case 'B':
                            case 'C':
                            case 'D':
                            case 'E':
                            case 'F':
                                value = (value << 4) + 10 + aChar - 'A';
                                break;
                            default:
                                throw new IllegalArgumentException(
                                        "Malformed   \\uxxxx   encoding.");
                        }

                    }
                    outBuffer.append((char) value);
                } else {
                    if (aChar == 't')
                        aChar = '\t';
                    else if (aChar == 'r')
                        aChar = '\r';
                    else if (aChar == 'n')
                        aChar = '\n';
                    else if (aChar == 'f')
                        aChar = '\f';
                    outBuffer.append(aChar);
                }
            } else
                outBuffer.append(aChar);
        }
        return outBuffer.toString();
    }

    /**
     * 获取消息回溯的配置文件. /conf/application.conf
     * <p>
     * 注意获取该配置文件后会对原文件进行删除...
     * <p>
     * 只会 load 一次，因为第二次文件已经删除
     * <p>
     * 环境变量: {@code msg.back.tracking.path} 指定 path
     */
    public static synchronized Map<String, ResumeConfig> loadMsgBackConfig() {
        if (!loadFlag) {
            resumeConfigList.putAll(ConfigLoader.load(SysEnvUtil.MSG_BACK_TRACKING));
            loadFlag = true;
        }
        return Collections.unmodifiableMap(resumeConfigList);
    }

    public static String combineResumeKey(String groupId, String topic, Integer partition) {
        return groupId + topic + partition;
    }

}
