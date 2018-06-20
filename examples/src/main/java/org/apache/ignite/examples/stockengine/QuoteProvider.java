package org.apache.ignite.examples.stockengine;

import org.apache.ignite.examples.stockengine.domain.Instrument;
import org.apache.ignite.examples.stockengine.domain.Quote;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class QuoteProvider {
    public interface Listener {
        void listen(Quote quote);
    }

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public void registerListener(Instrument instrument, Listener listener) {
        listeners.add(listener);
    }

    public void start() {
        new Thread(new Runnable() {
            @Override public void run() {
                while (!stopped.get()) {
                    try {
                        for (Listener listener : listeners)
                            listener.listen(new Quote(new Instrument("EU"), 1.0003, 1.0005, System.currentTimeMillis()));

                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    public void stop() {
        stopped.set(true);
    }
}
