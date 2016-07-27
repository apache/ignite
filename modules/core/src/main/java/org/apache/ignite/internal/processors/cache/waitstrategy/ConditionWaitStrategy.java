package org.apache.ignite.internal.processors.cache.waitstrategy;


import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.typedef.internal.U;

public class ConditionWaitStrategy implements WaitStrategy {
    /** Mutex. */
    private final Object mux = new Object();

    /** Next expire time. */
    private volatile long nextExpireTime;

//    private final AtomicInteger counter = new AtomicInteger();


    @Override public void notify0(long expireTime, Thread thread) {
        if (expireTime < nextExpireTime) {
//            System.out.println(counter.incrementAndGet());
            synchronized (mux) {
                mux.notifyAll();
            }
        }
    }

    @Override public void waitFor(long expireTime) throws InterruptedException {
        long curTime = U.currentTimeMillis();

        long waitTime = expireTime != -1 ? expireTime - curTime : 500;

        if (waitTime > 0) {
            synchronized (mux) {
                nextExpireTime = expireTime != -1 ? expireTime : curTime + 500;

                mux.wait(waitTime);
            }
        }
    }
}