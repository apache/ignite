package org.apache.ignite.internal.processors.security.sandbox;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;

public class PrivateUtilsCallable implements IgniteCallable {
    @Override public Object call() throws Exception {
        return U.hexLong(101L);
    }
}
