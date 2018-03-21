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

package org.apache.ignite.internal.failure;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Specific implementation that could be used only with ignite.(sh|bat).
 * Process must be terminated using Ignition.restart(true) call.
 */
public class RestartProcessFailureHandler implements FailureHandler {
    /** {@inheritDoc} */
    @Override public boolean onFailure(FailureContext failureCtx, Ignite ignite) {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    final IgniteLogger log = ignite.log();

                    U.warn(log, "Restarting JVM on Ignite failure: " + failureCtx);

                    G.restart(true);
                }
            },
            "node-restarter"
        ).start();

        return true;
    }
}
