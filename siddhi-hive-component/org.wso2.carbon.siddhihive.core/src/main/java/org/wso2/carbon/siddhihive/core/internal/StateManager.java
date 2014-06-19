package org.wso2.carbon.siddhihive.core.internal;


import org.wso2.carbon.siddhihive.core.configurations.Context;

public class StateManager {
    private static final ThreadLocal<Context> context = new ThreadLocal<Context>();

    public static void setContext(Context param) {
        context.set(param);
    }

    public static Context getContext() {
        return context.get();
    }

    public static void removeContext() {
        context.remove();
    }
}
