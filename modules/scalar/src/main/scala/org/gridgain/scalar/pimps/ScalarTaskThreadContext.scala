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

import org.apache.ignite.cluster.ClusterGroup
import org.gridgain.grid._
import org.gridgain.scalar._
import org.jetbrains.annotations._

/**
 * This trait provide mixin for properly typed version of `GridProjection#with...()` methods.
 *
 * Method on `GridProjection` always returns an instance of type `GridProjection` even when
 * called on a sub-class. This trait's methods return the instance of the same type
 * it was called on.
 */
trait ScalarTaskThreadContext[T <: ClusterGroup] extends ScalarConversions { this: PimpedType[T] =>
    /**
     * Properly typed version of `GridCompute#withName(...)` method.
     *
     * @param taskName Name of the task.
     */
    def withName$(@Nullable taskName: String): T =
        value.grid().compute(value).withName(taskName).asInstanceOf[T]

    /**
     * Properly typed version of `GridCompute#withNoFailover()` method.
     */
    def withNoFailover$(): T =
        value.grid().compute(value).withNoFailover().asInstanceOf[T]
}
