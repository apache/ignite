package org.apache.ignite.p2p.security;

import java.security.PrivilegedExceptionAction;

public class RunnableAction implements PrivilegedExceptionAction<Void> {

    private final Runnable r;

    public RunnableAction(Runnable r) {
        this.r = r;
    }

    @Override public Void run() throws Exception {
        r.run();

        return null;
    }
}
