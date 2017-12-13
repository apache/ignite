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

package org.apache.ignite.failover;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class IgniteFailureProcessor {
    /** Instance. */
    public static final IgniteFailureProcessor INSTANCE = new IgniteFailureProcessor();

    /**
     * @param ctx Context.
     * @param type Type.
     * @param cause Cause.
     */
    public void processFailure(GridKernalContext ctx, IgniteFailureCause.Type type, Throwable cause) {
        final IgniteFailureHandler hnd = ctx.config().getIgniteFailureHandler();

        final IgniteFailureAction act = hnd.onFailure(ctx, new IgniteFailureCause(type, cause));

        final IgniteLogger log = ctx.log(getClass());

        switch (act) {
            case RESTART_JVM:
                U.warn(log, "Restarting JVM on Ignite failure of type " + type);

                restartJvm();

                break;

            case STOP:
                U.warn(log, "Stopping local node on Ignite failure of type " + type);

                stopNode(ctx);

                break;

            default:
                assert act == IgniteFailureAction.NOOP : "Unsupported ignite failure action value: " + act;
        }
    }

    /**
     * @param ctx Context.
     * @param type Type.
     */
    public void processFailure(GridKernalContext ctx, IgniteFailureCause.Type type) {
        processFailure(ctx, type, null);
    }

    /** Restarts JVM. */
    private void restartJvm() {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    G.restart(true);
                }
            }
        ).start();
    }

    /** Stops local node. */
    private void stopNode(final GridKernalContext ctx) {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    G.stop(ctx.igniteInstanceName(), true);
                }
            }
        ).start();
    }
}
