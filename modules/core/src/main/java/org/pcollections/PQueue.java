/**
 *
 */
package org.pcollections;

import java.util.*;

/**
 * A persistent queue.
 *
 * @author mtklein
 */
public interface PQueue<E> extends PCollection<E>, Queue<E> {
    // TODO i think PQueue should extend PSequence,
    // even though the methods will be inefficient -- H

    /* Guaranteed to stay as a PQueue, i.e. guaranteed-fast methods */
    public PQueue<E> minus();

    public PQueue<E> plus(E e);

    public PQueue<E> plusAll(Collection<? extends E> list);


    /* May switch to other PCollection, i.e. may-be-slow methods */
    public PCollection<E> minus(Object e);

    public PCollection<E> minusAll(Collection<?> list);

    @Deprecated
    boolean offer(E o);

    @Deprecated
    E poll();

    @Deprecated
    E remove();
}
