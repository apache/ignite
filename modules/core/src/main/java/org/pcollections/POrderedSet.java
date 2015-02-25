package org.pcollections;

import java.util.*;

/**
 * Like {@link org.pcollections.PSet} but preserves insertion order. Persistent equivalent of
 * {@link java.util.LinkedHashSet}.
 *
 * @param <E>
 * @author Tassilo Horn &lt;horn@uni-koblenz.de&gt;
 */
public interface POrderedSet<E> extends PSet<E> {

    public POrderedSet<E> plus(E e);

    public POrderedSet<E> plusAll(Collection<? extends E> list);

    public POrderedSet<E> minus(Object e);

    public POrderedSet<E> minusAll(Collection<?> list);

    E get(int index);

    int indexOf(Object o);
}
