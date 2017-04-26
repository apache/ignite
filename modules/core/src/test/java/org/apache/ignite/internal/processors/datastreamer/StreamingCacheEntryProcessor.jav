package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.cache.CacheEntryProcessor;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

public class StreamingCacheEntryProcessor implements CacheEntryProcessor<String, Object, Object> {

    @Override
    public Object process(MutableEntry<String, Object> entry, Object... arguments) throws EntryProcessorException {
        return "OK";
    }
}
