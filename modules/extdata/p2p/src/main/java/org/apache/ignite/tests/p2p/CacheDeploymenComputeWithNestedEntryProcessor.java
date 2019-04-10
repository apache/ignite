package org.apache.ignite.tests.p2p;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

public class CacheDeploymenComputeWithNestedEntryProcessor implements IgniteCallable<Boolean> {
    /** */
    @IgniteInstanceResource
    Ignite ignite;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Key. */
    private int key;

    /** Cache name. */
    private String cacheName;

    /**
     * Default constructor.
     */
    public CacheDeploymenComputeWithNestedEntryProcessor() {
        // No-op.
    }

    /**
     * @param param Parameter.
     */
    public CacheDeploymenComputeWithNestedEntryProcessor(String cacheName, int key) {
        this.key = key;
        this.cacheName = cacheName;
    }

    /** {@inheritDoc} */
    @Override public Boolean call() throws Exception {
        log.info("!!!!! I am Compute with nested entry processor " + key + " on " + ignite.name());

        return ignite.cache(cacheName).withKeepBinary().invoke(key, new NestedEntryProcessor());
    }

    /** */
    private static class NestedEntryProcessor implements EntryProcessor<Object, Object, Boolean> {

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            return entry.getValue() != null;
        }
    }
}
