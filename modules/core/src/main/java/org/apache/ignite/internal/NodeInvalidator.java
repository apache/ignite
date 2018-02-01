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
package org.apache.ignite.internal;

import org.apache.ignite.IgniteLogger;
import org.jetbrains.annotations.NotNull;

/**
 * Temporary functionality to invalidate and stop the node.
 * TODO: Should be replaced on proper implementation in https://issues.apache.org/jira/browse/IGNITE-6891
 */
public class NodeInvalidator {
    public static NodeInvalidator INSTANCE = new NodeInvalidator();

    private static final long STOP_TIMEOUT_MS = 60 * 1000;

    private NodeInvalidator() {
        // Empty
    }

    public void invalidate(@NotNull GridKernalContext ctx, @NotNull Throwable error) {
        if (ctx.invalidated())
            return;

        ctx.invalidate();

        final String gridName = ctx.igniteInstanceName();
        final IgniteLogger logger = ctx.log(getClass());

        logger.error("Critical error with " + gridName + " is happened. " +
                "All further operations will be failed and local node will be stopped.", error);

        new Thread("node-stopper") {
            @Override public void run() {
                IgnitionEx.stop(gridName, true, true, STOP_TIMEOUT_MS);
            }
        }.start();
    }
}
