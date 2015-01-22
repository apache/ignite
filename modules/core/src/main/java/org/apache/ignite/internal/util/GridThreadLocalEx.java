/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Thread local that auto resets upon leaving thread context. This thread local is different
 * from {@link GridThreadLocal} as it inherits thread local values from the parent
 * {@link GridWorker} thread.
 */
public class GridThreadLocalEx<T> extends ThreadLocal<T> {
    /** Thread context for non-worker threads. */
    private static final ThreadLocal<ThreadContext> threadCtx = new ThreadLocal<ThreadContext>() {
        @Override protected ThreadContext initialValue() {
            return new ThreadContext();
        }
    };

    /** */
    private final IgniteOutClosure<T> initializer;

    /**
     *
     */
    public GridThreadLocalEx() {
        initializer = null;
    }

    /**
     * @param initializer Initializer.
     */
    public GridThreadLocalEx(IgniteOutClosure<T> initializer) {
        this.initializer = initializer;
    }

    /**
     * @return Thread locals to inherit.
     */
    public static Map<GridThreadLocalEx<?>, ?> inherit() {
        ThreadContext ctx = threadCtx.get();

        Collection<GridThreadLocalEx<?>> threadLocals = ctx.threadLocals();

        Map<GridThreadLocalEx<?>, Object> ret = F.isEmpty(threadLocals) ?
            Collections.<GridThreadLocalEx<?>, Object>emptyMap() :
            U.<GridThreadLocalEx<?>, Object>newHashMap(threadLocals.size());

        for (GridThreadLocalEx<?> t : threadLocals)
            ret.put(t, t.get());

        return ret;
    }

    /**
     * Callback for start of thread context.
     */
    public static void enter() {
        threadCtx.get().enter();
    }

    /**
     * Callback for start of thread context.
     *
     * @param inherited Inherited map.
     */
    @SuppressWarnings({"unchecked"})
    public static void enter(Map<GridThreadLocalEx<?>, ?> inherited) {
        threadCtx.get().enter();

        for (Map.Entry<GridThreadLocalEx<?>, ?> e : inherited.entrySet()) {
            ThreadLocal<Object> t = (ThreadLocal<Object>)e.getKey();

            t.set(e.getValue());
        }
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
     * Resets the state of this thread local.
     */
    private void reset() {
        super.set(initialValue());
    }

    /**
     * @param threadLoc Thread local.
     * @return {@code True} if thread-local was added.
     */
    private boolean addThreadLocal(GridThreadLocalEx<?> threadLoc) {
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
        private Collection<GridThreadLocalEx<?>> threadLocals = new HashSet<>();

        /**
         * Enter callback.
         */
        void enter() {
            assert entered >= 0 : "Thread context gateway cannot be negative prior to enter: " + entered;

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
        boolean add(GridThreadLocalEx<?> threadLoc) {
            return threadLocals.add(threadLoc);
        }

        /**
         * @return Thread locals.
         */
        Collection<GridThreadLocalEx<?>> threadLocals() {
            return threadLocals;
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
                for (GridThreadLocalEx<?> threadLocal : threadLocals)
                    threadLocal.reset();

                threadLocals.clear();
            }
        }
    }
}
