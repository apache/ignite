package org.apache.ignite.internal.processors.cache.persistence.tree.util;

import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;

/**
 * Wrapper factory for {@link PageHandler} instances.
 *
 * @param <R> Result type of actual {@link PageHandler} class.
 */
public interface PageHandlerWrapper<R> {
    /**
     * Wraps given {@code hnd}.
     *
     * @param tree Instance of {@link BPlusTree} where given {@code} is used.
     * @param hnd Page handler to wrap.
     * @return Wrapped version of given {@code hnd}.
     */
    public PageHandler<?, R> wrap(BPlusTree<?, ?> tree, PageHandler<?, R> hnd);
}
