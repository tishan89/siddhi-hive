package org.wso2.carbon.siddhihive.core.utils;

/**
 * Created by firzhan on 6/18/14.
 */
public enum WindowProcessingState {

    WINDOW_PROCESSING(4),
    WINDOW_PROCESSED(5),
    NONE(6);

    private int code;

    private WindowProcessingState(int c) {
        code = c;
    }

    public int getCode() {
        return code;
    }
}
