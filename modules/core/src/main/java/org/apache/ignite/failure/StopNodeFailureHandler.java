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

package org.apache.ignite.failure;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Handler will stop node in case of critical error using {@code IgnitionEx.stop(nodeName, true, true)} call.
 */
public class StopNodeFailureHandler extends AbstractFailureHandler {
    /** {@inheritDoc} */
    @Override public boolean handle(Ignite ignite, FailureContext failureCtx) {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    U.error(ignite.log(), "Stopping local node on Ignite failure: [failureCtx=" + failureCtx + ']');

                    IgnitionEx.stop(ignite.name(), true, true);
                }
            },
            "node-stopper"
        ).start();

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StopNodeFailureHandler.class, this, "super", super.toString());
    }
}
