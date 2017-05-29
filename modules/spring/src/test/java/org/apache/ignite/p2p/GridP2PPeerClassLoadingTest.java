package org.apache.ignite.p2p;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

/**
 * @author Alexey Ivanov
 * @version 1.0, 18.04.2017
 */
public class GridP2PPeerClassLoadingTest extends GridCommonAbstractTest {
    /**
     * public static class StreamingExampleCacheEntryProcessor extends StreamTransformer<String, Long> {
     *
     * @Override public Object process(MutableEntry<String, Long> e, Object... arg) throws EntryProcessorException {
     * System.out.println("Executed!");
     * Long val = e.getValue();
     * e.setValue(val == null ? 1L : val + 1);
     * return null;
     * }
     * }
     */

    public static class StreamingExampleCacheEntryProcessor implements CacheEntryProcessor<String, Long, Object> {
        @Override
        public Object process(MutableEntry<String, Long> e, Object... arg) throws EntryProcessorException {
            Long val = e.getValue();
            e.setValue(val == null ? 1L : val + 1);
            return null;
        }
    }


//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
//        IgniteConfiguration cfg = super.getConfiguration(gridName);
//
////        if (gridName.contains("client")) {
////            cfg.setClientMode(true);
////        }
////        cfg.setPeerClassLoadingEnabled(true);
//
//        return cfg;
//    }

    /**
     * Process one test.
     */
    public void testGridP2PPeerClassLoading() throws Exception {

        Ignite server = startGrid("server");

        Ignition.setClientMode(true);
        Ignite client = Ignition.start("examples\\config\\example-ignite.xml");

        IgniteCache<String, Long> stmCache = client.getOrCreateCache("mycache");
        IgniteDataStreamer<String, Long> stmr = client.dataStreamer(stmCache.getName());
        stmr.allowOverwrite(true);
        stmr.receiver(StreamTransformer.from(new StreamingExampleCacheEntryProcessor()));
//                stmr.receiver(new StreamingExampleCacheEntryProcessor());
        stmr.addData("word", 1L);
        System.out.println("Finished");
        stopGrid(server.name());
    }
}
