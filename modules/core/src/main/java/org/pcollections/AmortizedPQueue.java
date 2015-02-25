package org.pcollections;

import java.util.*;

// TODO javadoc
// TODO tests

/**
 * @param <E>
 * @author mtklein
 */
public class AmortizedPQueue<E> extends AbstractQueue<E> implements PQueue<E> {

    private static final AmortizedPQueue<Object> EMPTY = new AmortizedPQueue<Object>();
    private final PStack<E> front;
    private final PStack<E> back;
    private AmortizedPQueue() {
        front = Empty.<E>stack();
        back = Empty.<E>stack();
    }

    private AmortizedPQueue(AmortizedPQueue<E> queue, E e) {
        /* Guarantee that there is always at least 1 element
		 * in front, which makes peek worst-case O(1).
		 */
        if (queue.front.size() == 0) {
            this.front = queue.front.plus(e);
            this.back = queue.back;
        }
        else {
            this.front = queue.front;
            this.back = queue.back.plus(e);
        }
    }

    private AmortizedPQueue(PStack<E> front, PStack<E> back) {
        this.front = front;
        this.back = back;
    }

    @SuppressWarnings("unchecked")
    public static <E> AmortizedPQueue<E> empty() {
        return (AmortizedPQueue<E>) EMPTY;
    }

    public static void main(String[] args) {
        AmortizedPQueue<Integer> queue = new AmortizedPQueue<Integer>();

        queue = queue.plus(1).minus().minus().plus(2).plus(3).plus(4).plus(5).minus().plus(6).plus(7);
        PQueue<Integer> original = queue;

        System.out.println("    \t" + queue.front + " " + queue.back);

        while (queue.size() > 0) {
            int i = queue.peek();
            queue = queue.minus();
            System.out.println(i + " <- \t" + queue.front + " " + queue.back);
        }

        System.out.println(original);
    }

    /* Worst-case O(n) */
    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {
            private PQueue<E> queue = AmortizedPQueue.this;

            public boolean hasNext() {
                return queue.size() > 0;
            }

            public E next() {
                E e = queue.peek();
                queue = queue.minus();
                return e;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /* Worst-case O(1) */
    @Override
    public int size() {
        return front.size() + back.size();
    }

    /* Worst-case O(1) */
    public E peek() {
        if (size() == 0)
            return null;
        return front.get(0);
    }

    /* Amortized O(1), worst-case O(n) */
    public AmortizedPQueue<E> minus() {
        if (size() == 0)
            return this;

        int fsize = front.size();

        if (fsize == 0) {
            //If there's nothing on front, dump back onto front
            //(as stacks, this goes in reverse like we want)
            //and take one off.
            return new AmortizedPQueue<E>(Empty.<E>stack().plusAll(back), Empty.<E>stack()).minus();
        }
        else if (fsize == 1) {
            //If there's one element on front, dump back onto front,
            //but now we've already removed the head.
            return new AmortizedPQueue<E>(Empty.<E>stack().plusAll(back), Empty.<E>stack());
        }
        else {
            //If there's more than one on front, we pop one off.
            return new AmortizedPQueue<E>(front.minus(0), back);
        }
    }

    /* Worst-case O(1) */
    public AmortizedPQueue<E> plus(E e) {
        return new AmortizedPQueue<E>(this, e);
    }

    /* Worst-case O(k) */
    public AmortizedPQueue<E> plusAll(Collection<? extends E> list) {
        AmortizedPQueue<E> result = this;
        for (E e : list) {
            result = result.plus(e);
        }
        return result;
    }

    /* These 2 methods not guaranteed to be fast.*/
    public PCollection<E> minus(Object e) {
        return Empty.<E>vector().plusAll(this).minus(e);
    }

    public PCollection<E> minusAll(Collection<?> list) {
        return Empty.<E>vector().plusAll(this).minusAll(list);
    }

    /* These 2 methods are not applicable to a persistent collection. */
    public boolean offer(E o) {
        // Not possible to modify a persistent queue, interface
        // says return false if it's not added.
        throw new UnsupportedOperationException();
    }

    public E poll() {
        // Poll is meant to destructively remove and return the front of the queue.
        throw new UnsupportedOperationException();
    }
}
