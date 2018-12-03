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

package org.apache.ignite.internal.processors.security;

import java.util.ArrayDeque;
import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.jetbrains.annotations.Nullable;

/**
 * Current remote initiator holder.
 */
public final class CurrentRemoteInitiator {
    /** Stack. */
    private static final ThreadLocalStack<RemoteInitiator> stack = new ThreadLocalStack<>();

    /**
     * Set Remote Initaitor to holder.
     *
     * @param ctx Kernal context.
     * @param nodeId Node id.
     */
    public static void set(GridKernalContext ctx, UUID nodeId) {
        if (!ctx.localNodeId().equals(nodeId))
            stack.push(new RemoteInitiator(nodeId));
    }

    /**
     * Set Remote Initaitor to holder.
     *
     * @param secCtx Security context.
     */
    public static void set(SecurityContext secCtx) {
        stack.push(new RemoteInitiator(secCtx));
    }

    /**
     * Getting current Remote Initiator.
     *
     * @return Node id.
     */
    public static @Nullable RemoteInitiator get() {
        return stack.peek();
    }

    /**
     * Remove Remote Initiator if passed id isn't local node id.
     *
     * @param ctx Kernal context.
     * @param nodeId Node's id.
     */
    public static void remove(GridKernalContext ctx, UUID nodeId) {
        if (!ctx.localNodeId().equals(nodeId))
            stack.pop();
    }

    /**
     * Remove Remote Initiator if passed id isn't local node id.
     */
    public static void remove() {
        stack.pop();
    }

    private static class ThreadLocalStack<T> {
        /** Local. */
        private final ThreadLocal<ArrayDeque<T>> loc = ThreadLocal.withInitial(ArrayDeque::new);

        /**
         * {@link java.util.ArrayDeque#push(Object)}
         */
        public void push(T item) {
            assert item != null;

            loc.get().push(item);
        }

        /**
         * {@link java.util.ArrayDeque#peek()}
         */
        public T peek() {
            return loc.get().peek();
        }

        /**
         * {@link java.util.ArrayDeque#pop()}
         */
        public T pop() {
            return loc.get().pop();
        }

        /**
         * @return True if stack is empty.
         */
        public boolean isEmpty() {
            return loc.get().isEmpty();
        }
    }
}
