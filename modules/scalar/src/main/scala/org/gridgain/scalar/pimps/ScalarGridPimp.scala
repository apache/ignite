/* @scala.file.header */

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.pimps

import org.apache.ignite.{IgniteCluster, Ignite}
import org.gridgain.grid._
import org.jetbrains.annotations.Nullable
import org.gridgain.grid.scheduler.SchedulerFuture

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
 * Scala GridGain counterparts in `ScalarConversions` object
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

        value.grid().scheduler().scheduleLocal(toCallable(s), ptrn)
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

        value.grid().scheduler().scheduleLocal(toRunnable(s), ptrn)
    }
}
