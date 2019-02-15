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

package org.apache.ignite.scalar.pimps

import org.apache.ignite.scheduler.SchedulerFuture
import org.apache.ignite.{Ignite, IgniteCluster}
import org.jetbrains.annotations.Nullable

/**
 * Companion object.
 */
object ScalarGridPimp {
    /**
     * Creates new Scalar grid pimp with given Java-side implementation.
     *
     * @param impl Java-side implementation.
     */
    def apply(impl: Ignite) = {
        if (impl == null)
            throw new NullPointerException("impl")

        val pimp = new ScalarGridPimp

        pimp.impl = impl.cluster()

        pimp
    }
}

/**
 * ==Overview==
 * Defines Scalar "pimp" for `Grid` on Java side.
 *
 * Essentially this class extends Java `GridProjection` interface with Scala specific
 * API adapters using primarily implicit conversions defined in `ScalarConversions` object. What
 * it means is that you can use functions defined in this class on object
 * of Java `GridProjection` type. Scala will automatically (implicitly) convert it into
 * Scalar's pimp and replace the original call with a call on that pimp.
 *
 * Note that Scalar provide extensive library of implicit conversion between Java and
 * Scala Ignite counterparts in `ScalarConversions` object
 *
 * ==Suffix '$' In Names==
 * Symbol `$` is used in names when they conflict with the names in the base Java class
 * that Scala pimp is shadowing or with Java package name that your Scala code is importing.
 * Instead of giving two different names to the same function we've decided to simply mark
 * Scala's side method with `$` suffix.
 */
class ScalarGridPimp extends ScalarProjectionPimp[IgniteCluster] with ScalarTaskThreadContext[IgniteCluster] {
    /**
     * Schedules closure for execution using local cron-based scheduling.
     *
     * @param s Closure to schedule to run as a background cron-based job.
     * @param ptrn  Scheduling pattern in UNIX cron format with optional prefix `{n1, n2}`
     *     where `n1` is delay of scheduling in seconds and `n2` is the number of execution. Both
     *     parameters are optional.
     */
    def scheduleLocalCall[R](@Nullable s: Call[R], ptrn: String): SchedulerFuture[R] = {
        assert(ptrn != null)

        value.ignite().scheduler().scheduleLocal(toCallable(s), ptrn)
    }

    /**
     * Schedules closure for execution using local cron-based scheduling.
     *
     * @param s Closure to schedule to run as a background cron-based job.
     * @param ptrn  Scheduling pattern in UNIX cron format with optional prefix `{n1, n2}`
     *     where `n1` is delay of scheduling in seconds and `n2` is the number of execution. Both
     *     parameters are optional.
     */
    def scheduleLocalRun(@Nullable s: Run, ptrn: String): SchedulerFuture[_] = {
        assert(ptrn != null)

        value.ignite().scheduler().scheduleLocal(toRunnable(s), ptrn)
    }
}
