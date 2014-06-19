package org.wso2.carbon.siddhihive.core.utils.enums;

/**
 * Created by firzhan on 6/19/14.
 */
public enum WindowProcessingLevel {

    WND_SELECT_PROCESSING(4),
    WND_WHERE_PROCESSING(5),
    NONE(6);

    private int code;

    private WindowProcessingLevel(int c) {
        code = c;
    }

    public int getCode() {
        return code;
    }
}
