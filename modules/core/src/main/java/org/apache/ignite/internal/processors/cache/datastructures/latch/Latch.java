package org.apache.ignite.internal.processors.cache.datastructures.latch;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;

public interface Latch {

    void countDown();

    void await() throws IgniteCheckedException;

    void await(long time, TimeUnit timeUnit) throws IgniteCheckedException;

}
