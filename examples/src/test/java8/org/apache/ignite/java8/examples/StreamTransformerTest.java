package org.apache.ignite.java8.examples;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;


public class StreamTransformerTest extends GridCommonAbstractTest {
    public static class StreamingExampleCacheEntryProcessor implements CacheEntryProcessor<String, Long, Object> {
        @Override
        public Object process(MutableEntry<String, Long> e, Object... arg) throws EntryProcessorException {
            Long val = e.getValue();
            e.setValue(val == null ? 1L : val + 1);
            return null;
        }
    }

    public void test() throws Exception {
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            IgniteCache<String, Long> stmCache = ignite.getOrCreateCache("mycache");
            try (IgniteDataStreamer<String, Long> stmr = ignite.dataStreamer(stmCache.getName())) {
                stmr.allowOverwrite(true);
                stmr.receiver(StreamTransformer.from(new StreamingExampleCacheEntryProcessor()));
                stmr.addData("word", 1L);
                System.out.println("Finished");
            }
        }
    }

}
