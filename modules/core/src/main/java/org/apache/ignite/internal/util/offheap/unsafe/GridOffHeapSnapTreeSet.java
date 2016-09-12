package org.apache.ignite.internal.util.offheap.unsafe;

import org.jetbrains.annotations.Nullable;

import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Sorted set based on {@link GridOffHeapSnapTreeMap}
 */
public class GridOffHeapSnapTreeSet<E extends GridOffHeapSmartPointer> extends AbstractSet<E>
    implements SortedSet<E> {

    /** */
    private static final DummySmartPointer DUMMY_SMART_POINTER = new DummySmartPointer(Long.MAX_VALUE);

    /** */
    private final ConcurrentNavigableMap<E, DummySmartPointer> m;

    /**
     * Default constructor.
     */
    public GridOffHeapSnapTreeSet(GridOffHeapSmartPointerFactory<E> factory, GridUnsafeMemory mem,
        GridUnsafeGuard guard) {
        m = new GridOffHeapSnapTreeMap<>(factory, new ValueSmartPointerFactory(), mem, guard);
    }

    /** Contructor for sub sets */
    private GridOffHeapSnapTreeSet(ConcurrentNavigableMap<E, DummySmartPointer> map) {
        this.m = map;
    }

    /** {@inheritDoc} */
    @Override public Iterator<E> iterator() {
        return m.keySet().iterator();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return m.size();
    }

    /** {@inheritDoc} */
    @Override public SortedSet<E> subSet(E fromElement, E toElement) {
        return new GridOffHeapSnapTreeSet<>(m.subMap(fromElement, toElement));
    }

    /** {@inheritDoc} */
    @Override public SortedSet<E> headSet(E toElement) {
        return new GridOffHeapSnapTreeSet<>(m.headMap(toElement));
    }

    /** {@inheritDoc} */
    @Override public SortedSet<E> tailSet(E fromElement) {
        return new GridOffHeapSnapTreeSet<>(m.tailMap(fromElement));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Comparator<? super E> comparator() {
        return m.comparator();
    }

    /** {@inheritDoc} */
    @Override public E first() {
        return m.firstKey();
    }

    /** {@inheritDoc} */
    @Override public E last() {
        return m.lastKey();
    }

    @Override public boolean add(E e) {
        return m.put(e, DUMMY_SMART_POINTER) == null;
    }

    /**
     * Same as {@link #first()}, but returns {@code null} if set is empty.
     *
     * @return First entry or {@code null} if set is empty.
     */
    @Nullable public E firstx() {
        Map.Entry<E, DummySmartPointer> e = m.firstEntry();

        return e == null || e.getValue() == DUMMY_SMART_POINTER ? null : e.getKey();
    }

    @Override public boolean remove(Object o) {
        return m.remove(o) != null;
    }

    /**
     * Value SmartPointer factory
     */
    private static class ValueSmartPointerFactory implements GridOffHeapSmartPointerFactory {

        /** {@inheritDoc} */
        @Override public GridOffHeapSmartPointer createPointer(final long ptr) {
            assert ptr == Long.MAX_VALUE;
            return DUMMY_SMART_POINTER;
        }
    }

    /** Dummy smart pointer */
    private static class DummySmartPointer implements GridOffHeapSmartPointer {

        /** */
        private final long ptr;

        /**
         * @param ptr Unsafe memory pointer.
         */
        public DummySmartPointer(long ptr) {
            this.ptr = ptr;
        }

        /** {@inheritDoc} */
        @Override public long pointer() {
            return ptr;
        }

        /** {@inheritDoc} */
        @Override public void incrementRefCount() {
        }

        /** {@inheritDoc} */
        @Override public void decrementRefCount() {
        }
    }
}
