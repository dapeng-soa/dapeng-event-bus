package com.today.eventbus.utils;

/**
 * 描述: 封装 httpClient 返回结果
 *
 * @author hz.lei
 * @since 2018年05月12日 下午12:15
 */
public class ResponseResult {

    private int code;
    private String content;

    private Exception ex;

    public ResponseResult(int code, String content, Exception ex) {
        this.code = code;
        this.content = content;
        this.ex = ex;
    }

    public int getCode() {
        return code;
    }

    public String getContent() {
        return content;
    }

    public Exception getEx() {
        return ex;
    }
}
