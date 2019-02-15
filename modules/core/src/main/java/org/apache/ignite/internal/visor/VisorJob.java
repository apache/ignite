/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.visor;

import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.logFinish;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.logStart;

/**
 * Base class for Visor jobs.
 *
 * @param <A> Job argument type.
 * @param <R> Job result type.
 */
public abstract class VisorJob<A, R> extends ComputeJobAdapter {
    /** Auto-injected grid instance. */
    @IgniteInstanceResource
    protected transient IgniteEx ignite;

    /** Job start time. */
    protected transient long start;

    /** Debug flag. */
    protected boolean debug;

    /**
     * Create job with specified argument.
     *
     * @param arg Job argument.
     * @param debug Flag indicating whether debug information should be printed into node log.
     */
    protected VisorJob(@Nullable A arg, boolean debug) {
        super(arg);

        this.debug = debug;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object execute() {
        start = U.currentTimeMillis();

        A arg = argument(0);

        try {
            if (debug)
                logStart(ignite.log(), getClass(), start);

            return run(arg);
        }
        finally {
            if (debug)
                logFinish(ignite.log(), getClass(), start);
        }
    }

    /**
     * Execution logic of concrete job.
     *
     * @param arg Task argument.
     * @return Result.
     * @throws IgniteException In case of error.
     */
    protected abstract R run(@Nullable A arg) throws IgniteException;
}