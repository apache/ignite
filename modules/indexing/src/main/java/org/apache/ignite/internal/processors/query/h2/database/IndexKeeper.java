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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.List;

/**
 *
 */
public abstract class IndexKeeper {

    /** PageContext for use in IO's */
    private static final ThreadLocal<PageContext> currentIndex = new ThreadLocal<>();

    /**
     * @return Page context for current thread.
     */
    public static PageContext getContext() {
        return currentIndex.get();
    }

    /**
     * Sets page context for current thread.
     */
    public static void setContext(PageContext info) {
        currentIndex.set(info);
    }

    /**
     * Clears current context.
     */
    public static void clearContext() {
        currentIndex.remove();
    }

    /** */
    public static class PageContext {
        /** */
        private final List<InlineIndexHelper> inlineIdxs;

        /** */
        public PageContext(List<InlineIndexHelper> inlineIdxs) {
            this.inlineIdxs = inlineIdxs;
        }

        /** */
        public List<InlineIndexHelper> inlineIndexes() {
            return inlineIdxs;
        }
    }
}
