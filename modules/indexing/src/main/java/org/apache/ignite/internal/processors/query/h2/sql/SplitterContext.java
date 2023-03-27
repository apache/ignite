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

package org.apache.ignite.internal.processors.query.h2.sql;

import org.apache.ignite.internal.processors.query.h2.opt.join.CollocationModel;

/**
 * Splitter context while delegates optimization information to H2 internals.
 */
public class SplitterContext {
    /** Empty context. */
    private static final SplitterContext EMPTY = new SplitterContext(false);

    /** Splitter context. */
    private static final ThreadLocal<SplitterContext> CTX = ThreadLocal.withInitial(() -> EMPTY);

    /** Whether distributed joins are enabled. */
    private final boolean distributedJoins;

    /** Query collocation model. */
    private CollocationModel collocationModel;

    /**
     * @return Current context.
     */
    public static SplitterContext get() {
        return CTX.get();
    }

    /**
     * Set new context.
     *
     * @param distributedJoins Whether distributed joins are enabled.
     */
    public static void set(boolean distributedJoins) {
        SplitterContext ctx = distributedJoins ? new SplitterContext(true) : EMPTY;

        CTX.set(ctx);
    }

    /**
     * Constructor.
     *
     * @param distributedJoins Whether distributed joins are enabled.
     */
    public SplitterContext(boolean distributedJoins) {
        this.distributedJoins = distributedJoins;
    }

    /**
     * @return Whether distributed joins are enabled.
     */
    public boolean distributedJoins() {
        return distributedJoins;
    }

    /**
     * @return Query collocation model.
     */
    public CollocationModel collocationModel() {
        return collocationModel;
    }

    /**
     * @param collocationModel Query collocation model.
     */
    public void collocationModel(CollocationModel collocationModel) {
        this.collocationModel = collocationModel;
    }
}
