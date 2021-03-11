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

package org.apache.ignite.internal.cache.query.index.sorted;

/**
 * Holds an index row handler during work session with an index tree.
 */
public class ThreadLocalRowHandlerHolder {
    /** */
    private static final ThreadLocal<RowHandlerHolder> holder = ThreadLocal.withInitial(RowHandlerHolder::new);

    /** Set index row handler for current context. */
    public static void rowHandler(InlineIndexRowHandler rowHnd) {
        holder.get().rowHandler(rowHnd);
    }

    /** Get index row handler for current context. */
    public static InlineIndexRowHandler rowHandler() {
        return holder.get().rowHandler();
    }

    /** Clear index row handler for current context. */
    public static void clearRowHandler() {
        holder.get().clear();
    }

    /** Internal holder to avoid additional lookups of ThreadLocal set. */
    private static class RowHandlerHolder {
        /** Actual row handler. */
        private InlineIndexRowHandler rowHnd;

        /** */
        private void rowHandler(InlineIndexRowHandler rowHnd) {
            this.rowHnd = rowHnd;
        }

        /** */
        private InlineIndexRowHandler rowHandler() {
            return rowHnd;
        }

        /** */
        private void clear() {
            rowHnd = null;
        }
    }
}
