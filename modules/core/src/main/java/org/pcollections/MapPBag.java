package org.pcollections;

import java.util.*;
import java.util.Map.*;


/**
 * A map-backed persistent bag.
 * <p/>
 * If the backing map is thread-safe, then this implementation is thread-safe
 * (assuming Java's AbstractCollection is thread-safe), although its iterators
 * may not be.
 *
 * @param <E>
 * @author harold
 */
public final class MapPBag<E> extends AbstractCollection<E> implements PBag<E> {
//// STATIC FACTORY METHODS ////

    //// PRIVATE CONSTRUCTORS ////
    private final PMap<E, Integer> map;
    private final int size;
    // not instantiable (or subclassable):
    private MapPBag(final PMap<E, Integer> map, final int size) {
        this.map = map;
        this.size = size;
    }

    /**
     * @param <E>
     * @param map
     * @return a PBag backed by an empty version of map, i.e. by map.minusAll(map.keySet())
     */
    public static <E> MapPBag<E> empty(final PMap<E, Integer> map) {
        return new MapPBag<E>(map.minusAll(map.keySet()), 0);
    }

    //// PRIVATE STATIC UTILITIES ////
    private static int size(final PMap<?, Integer> map) {
        int size = 0;
        for (Integer n : map.values())
            size += n;
        return size;
    }

    //// REQUIRED METHODS FROM AbstractCollection ////
    @Override
    public int size() {
        return size;
    }

    @Override
    public Iterator<E> iterator() {
        final Iterator<Entry<E, Integer>> i = map.entrySet().iterator();
        return new Iterator<E>() {
            private E e;
            private int n = 0;

            public boolean hasNext() {
                return n > 0 || i.hasNext();
            }

            public E next() {
                if (n == 0) { // finished with current element
                    Entry<E, Integer> entry = i.next();
                    e = entry.getKey();
                    n = entry.getValue();
                }
                n--;
                return e;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    //// OVERRIDDEN METHODS OF AbstractCollection ////
    @Override
    public boolean contains(final Object e) {
        return map.containsKey(e);
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        for (E e : this)
            hashCode += e.hashCode();
        return hashCode;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object that) {
        if (!(that instanceof PBag))
            return false;
        if (!(that instanceof MapPBag)) {
            // make that into a MapPBag:
            // TODO this is INEFFICIENT
            MapPBag<Object> empty = (MapPBag<Object>) this.minusAll(this);
            that = empty.plusAll((PBag<?>) that);
        }
        return this.map.equals(((MapPBag<?>) that).map);
    }

    //// IMPLEMENTED METHODS OF PSet ////
    public MapPBag<E> plus(final E e) {
        return new MapPBag<E>(map.plus(e, count(e) + 1), size + 1);
    }

    @SuppressWarnings("unchecked")
    public MapPBag<E> minus(final Object e) {
        int n = count(e);
        if (n == 0)
            return this;
        if (n == 1) // remove from map
            return new MapPBag<E>(map.minus(e), size - 1);
        // otherwise just decrement count:
        return new MapPBag<E>(map.plus((E) e, n - 1), size - 1);
    }

    public MapPBag<E> plusAll(final Collection<? extends E> list) {
        MapPBag<E> bag = this;
        for (E e : list) bag = bag.plus(e);
        return bag;
    }

    public MapPBag<E> minusAll(final Collection<?> list) {
        // removes _all_ elements found in list, i.e. counts are irrelevant:
        PMap<E, Integer> map = this.map.minusAll(list);
        return new MapPBag<E>(map, size(map)); // (completely recomputes size)
    }

    //// PRIVATE UTILITIES ////
    // TODO should this be part of PBag?
    @SuppressWarnings("unchecked")
    private int count(final Object o) {
        if (!contains(o)) return 0;
        // otherwise o must be an E:
        return map.get((E) o);
    }
}
