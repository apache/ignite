package org.pcollections;

import java.util.*;

/**
 * An immutable, persistent collection of non-null elements of type E.
 *
 * @param <E>
 * @author harold
 */
public interface PCollection<E> extends Collection<E> {

    /**
     * @param e non-null
     * @return a collection which contains e and all of the elements of this
     */
    public PCollection<E> plus(E e);

    /**
     * @param list contains no null elements
     * @return a collection which contains all of the elements of list and this
     */
    public PCollection<E> plusAll(Collection<? extends E> list);

    /**
     * @param e
     * @return this with a single instance of e removed, if e is in this
     */
    public PCollection<E> minus(Object e);

    /**
     * @param list
     * @return this with all elements of list completely removed
     */
    public PCollection<E> minusAll(Collection<?> list);

    // TODO public PCollection<E> retainingAll(Collection<?> list);

    @Deprecated
    boolean add(E o);

    @Deprecated
    boolean remove(Object o);

    @Deprecated
    boolean addAll(Collection<? extends E> c);

    @Deprecated
    boolean removeAll(Collection<?> c);

    @Deprecated
    boolean retainAll(Collection<?> c);

    @Deprecated
    void clear();
}
