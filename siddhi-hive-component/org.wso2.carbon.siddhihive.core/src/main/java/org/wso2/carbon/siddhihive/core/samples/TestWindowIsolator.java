package org.wso2.carbon.siddhihive.core.samples;


import org.wso2.carbon.siddhihive.core.headerprocessor.WindowIsolator;

public class TestWindowIsolator {
    public static void main(String[] args) {
        WindowIsolator windowIsolator = new WindowIsolator();
        String testIncremental = windowIsolator.getIncrementalClause("stockQuote", "stockTable", true, 0);
        System.out.println(testIncremental);
    }
}
