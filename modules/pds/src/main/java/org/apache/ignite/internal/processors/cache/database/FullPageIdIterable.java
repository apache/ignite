package org.apache.ignite.internal.processors.cache.database;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class FullPageIdIterable implements PageIdIterable {
    /** (cacheId, partId) -> (lastAllocatedIndex, count) */
    private final NavigableMap<T2<Integer, Integer>, T2<Integer, Integer>> pageCounts;

    /** */
    private final int totalPageCount;

    /**
     * @param pageCounts Page counts map.
     */
    FullPageIdIterable(NavigableMap<T2<Integer, Integer>, T2<Integer, Integer>> pageCounts) {
        this.pageCounts = pageCounts;

        int sum = 0;

        for (T2<Integer, Integer> t2 : pageCounts.values())
            sum += t2.get2();

        totalPageCount = sum;
    }

    /**
     * @return Total page count.
     */
    public int size() {
        return totalPageCount;
    }

    /**
     * @param fullId Full page ID.
     * @return Total page count below the given page ID.
     */
    public int sizeBelow(FullPageId fullId) {
        if (fullId == null)
            return 0;

        T2<Integer, Integer> key = new T2<>(fullId.cacheId(), PageIdUtils.partId(fullId.pageId()));

        int sum = 0;

        for (T2<Integer, Integer> t2 : pageCounts.headMap(key).values())
            sum += t2.get2();

        sum += PageIdUtils.pageIndex(fullId.pageId());

        return sum;
    }

    /** {@inheritDoc} */
    @Override public Iterator<FullPageId> iterator() {
        return new FullPageIdIterator();
    }

    /**
     *
     * @param fullId Full page ID.
     * @return {@code True} if contains given ID.
     */
    @Override public boolean contains(FullPageId fullId) {
        int cacheId = fullId.cacheId();
        int partId = PageIdUtils.partId(fullId.pageId());
        long idx = PageIdUtils.pageIndex(fullId.pageId());

        T2<Integer, Integer> cnt = pageCounts.get(new T2<>(cacheId, partId));

        return cnt != null && cnt.get2() > idx;
    }

    /**
     *
     * @param fullId Full page ID.
     * @return Next element after the provided one. If provided element is the last one, returns null.
     * If provided element isn't contained, returns the first element.
     */
    @Override @Nullable public FullPageId next(@Nullable FullPageId fullId) {
        if (isEmpty())
            return null;

        if (fullId == null) {
            T2<Integer, Integer> firstKey = pageCounts.firstKey();

            return new FullPageId(PageIdUtils.pageId(firstKey.get2(), (byte)0, 0), firstKey.get1());
        }

        T2<Integer, Integer> key = new T2<>(fullId.cacheId(), PageIdUtils.partId(fullId.pageId()));
        int idx = PageIdUtils.pageIndex(fullId.pageId());

        T2<Integer, Integer> cnt = pageCounts.get(key);

        if (cnt == null || idx > cnt.get2() - 1) {
            T2<Integer, Integer> firstKey = pageCounts.firstKey();
            return new FullPageId(PageIdUtils.pageId(firstKey.get2(), (byte)0, 0), firstKey.get1());
        }
        else if (idx == cnt.get2() - 1) {
            NavigableMap<T2<Integer, Integer>, T2<Integer, Integer>> tailMap = pageCounts.tailMap(key, false);

            if (tailMap.isEmpty())
                return null;

            T2<Integer, Integer> firstKey = tailMap.firstKey();

            return new FullPageId(PageIdUtils.pageId(firstKey.get2(), (byte)0, 0), firstKey.get1());
        }
        else
            return new FullPageId(PageIdUtils.pageId(key.get2(), (byte)0, idx + 1), key.get1());
    }

    /** {@inheritDoc} */
    @Override public double progress(FullPageId current) {
        if (pageCounts.isEmpty())
            return 1;

        if (current == null)
            return 0;

        T2<Integer, Integer> key = new T2<>(current.cacheId(), PageIdUtils.partId(current.pageId()));

        return (double) pageCounts.headMap(key).size() / pageCounts.size();
    }

    /**
     *
     * @return {@code True} if empty.
     */
    @Override public boolean isEmpty() {
        return pageCounts.isEmpty();
    }

    /**
     *
     */
    private class FullPageIdIterator implements Iterator<FullPageId> {
        /** */
        private final Iterator<Map.Entry<T2<Integer, Integer>, T2<Integer, Integer>>> mapIter;

        /** */
        private Map.Entry<T2<Integer, Integer>, T2<Integer, Integer>> currEntry;

        /** */
        private int currIdx;

        /** */
        private FullPageId next;

        /** */
        private FullPageIdIterator() {
            mapIter = pageCounts.entrySet().iterator();

            advance();
        }

        /**
         *
         */
        private void advance() {
            if (currEntry == null && !mapIter.hasNext())
                return;
            else if (currEntry == null && mapIter.hasNext())
                currEntry = mapIter.next();
            else if (currIdx < currEntry.getValue().get2() - 1)
                currIdx++;
            else if (mapIter.hasNext()) {
                currEntry = mapIter.next();

                currIdx = 0;
            }
            else {
                next = null;

                return;
            }

            int cacheId = currEntry.getKey().get1();
            int partId = currEntry.getKey().get2();

            long pageId = PageIdUtils.pageId(partId, (byte)0, currIdx);

            next = new FullPageId(pageId, cacheId);
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return next != null;
        }

        /** {@inheritDoc} */
        @Override public FullPageId next() {
            FullPageId next = this.next;

            if (next == null)
                throw new NoSuchElementException();

            advance();

            return next;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }
}

/**
 *
 */
class FullPageIdIterableComparator implements Comparator<T2<Integer, Integer>>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    static final FullPageIdIterableComparator INSTANCE = new FullPageIdIterableComparator();

    /** {@inheritDoc} */
    @Override public int compare(T2<Integer, Integer> o1, T2<Integer, Integer> o2) {
        if (o1.get1() < o2.get1())
            return -1;

        if (o1.get1() > o2.get1())
            return 1;

        if (o1.get2() < o2.get2())
            return -1;

        if (o1.get2() > o2.get2())
            return 1;

        return 0;
    }
}

