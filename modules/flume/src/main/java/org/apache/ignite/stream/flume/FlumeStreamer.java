package org.apache.ignite.stream.flume;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;

/**
 * Flume streamer that receives events from a sink and feeds key-value pairs into an {@link IgniteDataStreamer}.
 */
public abstract class FlumeStreamer<Event, K, V> {
    private IgniteDataStreamer<K, V> dataStreamer;

    public IgniteDataStreamer<K, V> getDataStreamer() {
        return dataStreamer;
    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException if failed.
     */
    public void start(Ignite ignite, String cacheName) {
        ignite.getOrCreateCache(cacheName);
        dataStreamer = ignite.dataStreamer(cacheName);
    }

    /**
     * Stops streamer.
     */
    public void stop() {
        if (dataStreamer != null)
            dataStreamer.close();
    }

    /**
     * Writes a Flume event.
     *
     * @param event Flume event.
     */
    protected void writeEvent(Event event) {
        Map<K, V> entries = transform(event);

        if (entries == null || entries.size() == 0)
            return;

        dataStreamer.addData(entries);
    }

    /**
     * Transforms {@link Event} into key-values.
     *
     * @param event Flume event.
     * @return Map of transformed key-values.
     */
    protected abstract Map<K, V> transform(Event event);
}
