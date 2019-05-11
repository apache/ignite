package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import org.apache.ignite.lang.IgniteFuture;

public interface DumpSupported<T extends Dump> {

    boolean acquireSafePoint();

    boolean releaseSafePoint();

    T dump();

    IgniteFuture<T> dumpSync();
}
