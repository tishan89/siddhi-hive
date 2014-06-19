package org.wso2.carbon.siddhihive.core.utils.enums;

/**
 * Created by firzhan on 6/19/14.
 */
public enum WindowStreamProcessingLevel {

    TIME_WINDOW_PROCESSING(4),
    TIME_BATCH_WINDOW_PROCESSING(5),
    LENGTH_WINDOW_PROCESSING(5),
    LENGTH_BATCH_WINDOW_PROCESSING(6),
    WINDOW_PROCESSED(5),
    NONE(6);

    private int code;

    private WindowStreamProcessingLevel(int c) {
        code = c;
    }

    public int getCode() {
        return code;
    }
}
