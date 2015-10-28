package org.apache.ignite.stream.flume;

import java.util.HashMap;
import java.util.Map;
import org.apache.flume.Event;
import org.apache.ignite.Ignite;

/**
 * Creates a data streamer and implements an event transformation.
 */
public class TestFlumeStreamer extends FlumeStreamer<Event, String, Integer> {

    public void start(Ignite ignite, String cacheName) {
        super.start(ignite, cacheName);
        getDataStreamer().allowOverwrite(true);
        getDataStreamer().autoFlushFrequency(10);
    }

    @Override protected Map<String, Integer> transform(Event event) {
        final Map<String, Integer> map = new HashMap<>();
        String eventStr = new String(event.getBody());
        if (!eventStr.isEmpty()) {
            // expects column-delimited one line
            String[] tokens = eventStr.split(":");
            map.put(tokens[0].trim(), Integer.valueOf(tokens[1].trim()));
        }
        return map;
    }
}
