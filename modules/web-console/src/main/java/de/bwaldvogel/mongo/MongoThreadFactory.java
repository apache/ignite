package de.bwaldvogel.mongo;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class MongoThreadFactory implements ThreadFactory {

    private final AtomicLong counter = new AtomicLong();
    private final String prefix;

    public MongoThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName(prefix + counter.incrementAndGet());
        return thread;
    }

}
