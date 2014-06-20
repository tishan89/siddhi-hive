package org.wso2.carbon.siddhihive.core.utils.enums;

/**
 * Created by firzhan on 6/18/14.
 */
public enum ProcessingLevel {
    INPUT_STREAM(1),
    SELECTOR(2),
    OUTPUT_STREAM(5),
    NONE(6);

    private int code;

    private ProcessingLevel(int c) {
        code = c;
    }

    public int getCode() {
        return code;
    }
}



