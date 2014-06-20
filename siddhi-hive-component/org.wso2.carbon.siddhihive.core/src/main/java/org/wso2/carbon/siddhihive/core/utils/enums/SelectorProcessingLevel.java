package org.wso2.carbon.siddhihive.core.utils.enums;

/**
 * Created by firzhan on 6/19/14.
 */
public enum SelectorProcessingLevel {

    SELECTOR(1),
    GROUPBY(2),
    HAVING(5),
    NONE(6);


    private int code;

    private SelectorProcessingLevel(int c) {
        code = c;
    }

    public int getCode() {
        return code;
    }

}
