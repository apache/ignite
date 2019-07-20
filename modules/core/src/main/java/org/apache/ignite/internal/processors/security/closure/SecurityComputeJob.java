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

package org.apache.ignite.internal.processors.security.closure;

import java.util.Objects;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.internal.processors.security.IgniteSecurity;

/**
 * Wrapper for {@link ComputeJob} that executes its {@code execute} method with restriction defined by current security
 * context.
 *
 * @see IgniteSecurity#execute(java.util.concurrent.Callable)
 */
public class SecurityComputeJob implements ComputeJob {
    /** . */
    private static final long serialVersionUID = 7510836970476698602L;

    /** . */
    private final IgniteSecurity sec;

    /** . */
    private final ComputeJob origin;

    /** . */
    public SecurityComputeJob(IgniteSecurity sec, ComputeJob origin) {
        this.sec = Objects.requireNonNull(sec, "Sec cannot be null.");
        this.origin = Objects.requireNonNull(origin, "Origin cannot be null.");
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        origin.cancel();
    }

    /** {@inheritDoc} */
    @Override public Object execute() throws IgniteException {
        return sec.execute(origin::execute);
    }
}
