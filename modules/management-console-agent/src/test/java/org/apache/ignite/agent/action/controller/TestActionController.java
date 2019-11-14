/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.action.controller;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.agent.action.annotation.ActionController;
import org.apache.ignite.internal.GridKernalContext;

/**
 * Test action controller for other tests.
 */
@ActionController("IgniteTestActionController")
public class TestActionController {
    /**
     * Context.
     */
    private final GridKernalContext ctx;

    /**
     * @param ctx Context.
     */
    public TestActionController(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @param flag Flag.
     */
    public CompletableFuture<Boolean> action(boolean flag) {
        return CompletableFuture.completedFuture(flag);
    }

    /**
     * @param num Number.
     */
    public CompletableFuture<Boolean> numberAction(long num) {
        return CompletableFuture.completedFuture(true);
    }
}
