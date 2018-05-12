package com.today.eventbus.utils;

/**
 * 描述: 封装 httpClient 返回结果
 *
 * @author hz.lei
 * @date 2018年05月12日 下午12:15
 */
public class ResponseResult {

    private int code;
    private String content;

    public ResponseResult(int code, String content) {
        this.code = code;
        this.content = content;
    }

    public int getCode() {
        return code;
    }

    public String getContent() {
        return content;
    }
}
