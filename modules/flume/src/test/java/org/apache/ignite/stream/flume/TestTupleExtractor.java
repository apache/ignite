package org.apache.ignite.stream.flume;

import java.util.HashMap;
import java.util.Map;
import org.apache.flume.Event;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;

/**
 * An extractor to convert {@link org.apache.flume.Event} to {@link Map}.
 */
public class TestTupleExtractor implements StreamMultipleTupleExtractor<Event, String, Integer> {
    @Override public Map<String, Integer> extract(Event event) {
        final Map<String, Integer> map = new HashMap<>();
        String eventStr = new String(event.getBody());

        if (eventStr.isEmpty())
            return null;

        // expects column-delimited one line
        String[] tokens = eventStr.split(":");
        map.put(tokens[0].trim(), Integer.valueOf(tokens[1].trim()));
        return map;
    }
}
