/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Thread local that auto resets upon leaving thread context. This thread local is
 * integrated with {@link GridKernalGateway} and
 * with {@link GridWorker} threads.
 */
public class GridThreadLocal<T> extends ThreadLocal<T> {
    /** Thread context for non-worker threads. */
    private static final ThreadLocal<ThreadContext> threadCtx = new ThreadLocal<ThreadContext>() {
        @Override protected ThreadContext initialValue() {
            return new ThreadContext();
        }

        @Override public String toString() {
            return "Thread context.";
        }
    };

    /** */
    private final IgniteOutClosure<T> initializer;

    /**
     *
     */
    public GridThreadLocal() {
        initializer = null;
    }

    /**
     * @param initializer Initializer.
     */
    public GridThreadLocal(IgniteOutClosure<T> initializer) {
        this.initializer = initializer;
    }

    /**
     * Callback for start of thread context.
     */
    public static void enter() {
        threadCtx.get().enter();
    }

    /**
     * Callback for end of thread context.
     */
    public static void leave() {
        threadCtx.get().leave();
    }

    /** {@inheritDoc} */
    @Nullable @Override protected T initialValue() {
        return initializer == null ? null : initializer.apply();
    }

    /**
     * Resets the state of this thread local.
     */
    private void reset() {
        super.set(initialValue());
    }

    /** {@inheritDoc} */
    @Override public final T get() {
        addThreadLocal(this);

        return super.get();
    }

    /** {@inheritDoc} */
    @Override public final void set(T val) {
        if (val != null)
            addThreadLocal(this);

        super.set(val);
    }

    /**
     * @param threadLoc Thread local.
     * @return {@code True} if thread-local was added.
     */
    private boolean addThreadLocal(GridThreadLocal<?> threadLoc) {
        assert threadLoc != null;

        ThreadContext ctx = threadCtx.get();

        return ctx.entered() && ctx.add(threadLoc);
    }

    /**
     *
     */
    private static class ThreadContext {
        /** Entered flag. */
        private int entered;

        /** Thread locals for given thread context. */
        private Collection<GridThreadLocal<?>> threadLocals = new HashSet<>();

        /**
         * Enter callback.
         */
        void enter() {
            assert entered >= 0 : "Thread context gateway cannot be negative prior to enter: " + entered;

            if (entered == 0)
                reset();

            entered++;
        }

        /**
         * Leave callback.
         */
        void leave() {
            assert entered > 0 : "Thread context gateway must be positive prior to leave: " + entered;

            entered--;

            if (entered == 0)
                reset();
        }

        /**
         * @param threadLoc Thread local to add.
         * @return {@code True} if thread local was added.
         */
        boolean add(GridThreadLocal<?> threadLoc) {
            return threadLocals.add(threadLoc);
        }

        /**
         * @return Entered flag.
         */
        boolean entered() {
            return entered > 0;
        }

        /**
         * Resets thread locals.
         */
        private void reset() {
            if (!threadLocals.isEmpty()) {
                for (GridThreadLocal<?> threadLocal : threadLocals)
                    threadLocal.reset();

                threadLocals.clear();
            }
        }
    }
}
