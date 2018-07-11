package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.processors.query.h2.ObjectPool.Reusable;
import org.junit.Test;

import static org.junit.Assert.*;

public class ObjectPoolTest {
    private static class Obj implements AutoCloseable {
        private boolean closed = false;
        @Override
        public void close() {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    private ObjectPool<Obj> pool = new ObjectPool<>(Obj::new, 1);

    @Test
    public void connectionIsReusedAfterRecycling() {
        Reusable<Obj> o1 = pool.borrow();
        o1.recycle();
        Reusable<Obj> o2 = pool.borrow();

        assertSame(o1.object(), o2.object());
        assertFalse(o1.object().isClosed());
    }

    @Test
    public void borrowedConnectionIsNotReturnedTwice() {
        Reusable<Obj> o1 = pool.borrow();
        Reusable<Obj> o2 = pool.borrow();

        assertNotSame(o1.object(), o2.object());
    }

    @Test
    public void objectShouldBeClosedOnRecycleIfPoolIsFull() {
        Reusable<Obj> o1 = pool.borrow();
        Reusable<Obj> o2 = pool.borrow();
        o1.recycle();
        o2.recycle();

        assertTrue(o2.object().isClosed());
    }
}