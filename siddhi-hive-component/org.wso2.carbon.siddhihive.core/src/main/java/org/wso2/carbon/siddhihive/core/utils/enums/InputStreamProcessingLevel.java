package org.wso2.carbon.siddhihive.core.utils.enums;

/**
 * Created by firzhan on 6/19/14.
 */
public enum InputStreamProcessingLevel {

    BASIC_STREAM(1),
    WINDOW_STREAM(2),
    JOIN_STREAM(5),
    NONE(6);

    private int code;

    private InputStreamProcessingLevel(int c) {
        code = c;
    }

    public int getCode() {
        return code;
    }
}
