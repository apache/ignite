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

import org.apache.ignite.failure.IgniteFailureAction;
import org.apache.ignite.failure.IgniteFailureContext;
import org.apache.ignite.failure.IgniteFailureHandler;

/**
 * Default implementation of {@link IgniteFailureHandler}
 */
public class DefaultIgniteFailureHandler implements IgniteFailureHandler {
    /** {@inheritDoc} */
    @Override public IgniteFailureAction onFailure(IgniteFailureContext failureCtx) {
        switch (failureCtx.type()) {
            case SYSTEM_WORKER_CRASHED:
                return IgniteFailureAction.STOP;

            case CRITICAL_ERROR:
                return IgniteFailureAction.STOP;

            default:
                assert false : "Unsupported Ignite failure type: " + failureCtx.type();

                return IgniteFailureAction.STOP;
        }
    }
}
