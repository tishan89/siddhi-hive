package org.wso2.carbon.siddhihive.core.utils;

/**
 * Created by firzhan on 6/18/14.
 */
public enum ProcessingMode {
    INPUT_STREAM(1),
    SELECTOR(2),
    SELECTOR_HAVING(3),
    OUTPUT_STREAM(4);

    private int code;

    private ProcessingMode(int c) {
        code = c;
    }

    public int getCode() {
        return code;
    }
}



