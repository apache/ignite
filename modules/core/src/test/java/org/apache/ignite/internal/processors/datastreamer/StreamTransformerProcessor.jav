package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.stream.StreamTransformer;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

/**
 * Created by freem on 23.04.2017.
 */
public class StreamTransformerProcessor extends StreamTransformer{
    @Override
    public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
        return "OK";
    }
}
