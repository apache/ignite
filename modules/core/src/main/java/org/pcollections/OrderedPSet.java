package org.pcollections;

import java.util.*;

public class OrderedPSet<E> extends AbstractSet<E> implements POrderedSet<E> {
    private static final OrderedPSet<Object> EMPTY = new OrderedPSet<Object>(
        Empty.set(), Empty.vector());
    private PSet<E> contents;
    private PVector<E> order;

    private OrderedPSet(PSet<E> c, PVector<E> o) {
        contents = c;
        order = o;
    }

    @SuppressWarnings("unchecked")
    public static <E> OrderedPSet<E> empty() {
        return (OrderedPSet<E>) EMPTY;
    }

    @SuppressWarnings("unchecked")
    public static <E> OrderedPSet<E> from(final Collection<? extends E> list) {
        if (list instanceof OrderedPSet)
            return (OrderedPSet<E>) list;
        return OrderedPSet.<E>empty().plusAll(list);
    }

    public static <E> OrderedPSet<E> singleton(final E e) {
        return OrderedPSet.<E>empty().plus(e);
    }

    @Override
    public OrderedPSet<E> plus(E e) {
        if (contents.contains(e))
            return this;
        return new OrderedPSet<E>(contents.plus(e), order.plus(e));
    }

    @Override
    public OrderedPSet<E> plusAll(Collection<? extends E> list) {
        OrderedPSet<E> s = this;
        for (E e : list) {
            s = s.plus(e);
        }
        return s;
    }

    @Override
    public OrderedPSet<E> minus(Object e) {
        if (!contents.contains(e))
            return this;
        return new OrderedPSet<E>(contents.minus(e), order.minus(e));
    }

    @Override
    public OrderedPSet<E> minusAll(Collection<?> list) {
        OrderedPSet<E> s = this;
        for (Object e : list) {
            s = s.minus(e);
        }
        return s;
    }

    @Override
    public Iterator<E> iterator() {
        return order.iterator();
    }

    @Override
    public int size() {
        return contents.size();
    }

    @Override
    public E get(int index) {
        return order.get(index);
    }

    @Override
    public int indexOf(Object o) {
        if (!contents.contains(o))
            return -1;
        return order.indexOf(o);
    }
}
