package org.pcollections;

import java.util.*;


/**
 * A static convenience class for creating efficient persistent sets.
 * <p/>
 * This class simply creates MapPSets backed by HashTreePMaps.
 *
 * @author harold
 */
public final class HashTreePSet {
    private static final MapPSet<Object> EMPTY = MapPSet.from(HashTreePMap.empty());

    // not instantiable (or subclassable):
    private HashTreePSet() {
    }

    /**
     * @param <E>
     * @return an empty set
     */
    @SuppressWarnings("unchecked")
    public static <E> MapPSet<E> empty() {
        return (MapPSet<E>) EMPTY;
    }

    /**
     * @param <E>
     * @param e
     * @return empty().plus(e)
     */
    public static <E> MapPSet<E> singleton(final E e) {
        return HashTreePSet.<E>empty().plus(e);
    }

    /**
     * @param <E>
     * @param list
     * @return empty().plusAll(map)
     */
    public static <E> MapPSet<E> from(final Collection<? extends E> list) {
        return HashTreePSet.<E>empty().plusAll(list);
    }
}
