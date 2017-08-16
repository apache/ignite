package org.apache.ignite.examples.streaming;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

/**
 * Streaming example.
 *  <ul>
 *  <li>Start a remote ignite node using {@link ExampleNodeStartup}.</li>
 *  <li>Start streaming using {@link StreamingExample}.</li>
 *  </ul>
 */
public class StreamingExample {

    public static class StreamingExampleCacheEntryProcessor extends StreamTransformer<String, Long> {
        @Override
        public Object process(MutableEntry<String, Long> e, Object... arg) throws EntryProcessorException {
            System.out.println("Executed!");
            Long val = e.getValue();
            e.setValue(val == null ? 1L : val + 1);
            return null;
        }
    }

    public static void main(String[] args) throws IgniteException, IOException {
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            IgniteCache<String, Long> stmCache = ignite.getOrCreateCache("mycache");
            try (IgniteDataStreamer<String, Long> stmr = ignite.dataStreamer(stmCache.getName())) {
                stmr.allowOverwrite(true);
                stmr.receiver(new StreamingExampleCacheEntryProcessor());
                stmr.addData("word", 1L);
                System.out.println("Finished");
            }
        }
    }
}