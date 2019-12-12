package org.apache.ignite.internal.processors.security.sandbox;

import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.lang.IgniteCallable;

public class PrivateUtilsCallable implements IgniteCallable {
    @Override public Object call() throws Exception {
        return IgnitionEx.isDaemon();
    }
}
