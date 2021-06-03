/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache.persistence;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTreeRuntimeException;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.Nullable;

import static java.util.Arrays.asList;

/**
 * Abstract exception for exceptions related to persistence corruption.
 */
public abstract class AbstractCorruptedPersistenceException extends IgniteCheckedException implements CorruptedPersistenceException {
    /** */
    protected final T2<Integer, Long>[] pages;

    /**
     * @param msg Message.
     * @param cause Cause.
     * @param pages (groupId, pageId) pairs for pages that might be corrupted.
     */
    protected AbstractCorruptedPersistenceException(String msg, @Nullable Throwable cause, T2<Integer, Long>[] pages) {
        super(msg, cause);

        this.pages = expandPagesArray(pages, cause);
    }

    /**
     * @param grpId Group id.
     * @param pageIds Pages ids.
     * @return Pairs of (groupId, pageId).
     */
    protected static T2<Integer, Long>[] toPagesArray(int grpId, long[] pageIds) {
        T2<Integer, Long>[] res = (T2<Integer, Long>[])new T2[pageIds.length];

        for (int i = 0; i < pageIds.length; i++)
            res[i] = new T2<>(grpId, pageIds[i]);

        return res;
    }

    /**
     * Add partition meta pages and related pages.
     * @param pages Pages with group ids.
     * @param cause Cause exception.
     * @return Extended list of pages.
     */
    protected T2<Integer, Long>[] expandPagesArray(T2<Integer, Long>[] pages, Throwable cause) {
        Set<T2<Integer, Long>> res = new HashSet<>(asList(pages));

        BPlusTreeRuntimeException treeRuntimeException = X.cause(cause, BPlusTreeRuntimeException.class);

        // Add root exception pages ids if we have.
        if (treeRuntimeException != null)
            res.addAll(treeRuntimeException.pages());

        Set<T2<Integer, Long>> partMetaPages = partitionMetaPages(res);

        // Add meta pages for all (group,partition) pairs.
        res.addAll(partMetaPages);

        return (T2<Integer, Long>[])res.toArray(new T2[0]);
    }

    /**
     * @param pages Pages with group ids.
     * @return Partition meta pages with group ids, for given pages.
     */
    protected Set<T2<Integer, Long>> partitionMetaPages(Set<T2<Integer, Long>> pages) {
        return pages.stream().map(page -> {
            int grpId = page.get1();
            int partId = PageIdUtils.partId(page.get2());

            final long partMetaPageId = PageIdUtils.pageId(partId, PageIdAllocator.FLAG_DATA, 0);

            return new T2<>(grpId, partMetaPageId);
        }).collect(Collectors.toSet());
    }

    /** {@inheritDoc} */
    @Override public T2<Integer, Long>[] pages() {
        return pages;
    }
}
