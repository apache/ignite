package org.pcollections;

import java.util.*;

/**
 * An immutable, persistent list.
 *
 * @param <E>
 * @author harold
 */
public interface PVector<E> extends PSequence<E> {

    /**
     * Returns a vector consisting of the elements of this with e appended.
     */
    //@Override
    public PVector<E> plus(E e);

    /**
     * Returns a vector consisting of the elements of this with list appended.
     */
    //@Override
    public PVector<E> plusAll(Collection<? extends E> list);

    //@Override
    public PVector<E> with(int i, E e);

    //@Override
    public PVector<E> plus(int i, E e);

    //@Override
    public PVector<E> plusAll(int i, Collection<? extends E> list);

    //@Override
    public PVector<E> minus(Object e);

    //@Override
    public PVector<E> minusAll(Collection<?> list);

    //@Override
    public PVector<E> minus(int i);

    //@Override
    public PVector<E> subList(int start, int end);
}
