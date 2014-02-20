// @scala.file.header

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.pimps

import org.gridgain.grid._
import org.gridgain.scalar._
import org.jetbrains.annotations._
import org.gridgain.grid.compute.{GridComputeJobResultPolicy, GridComputeJobResult}

/**
 * This trait provide mixin for properly typed version of `GridProjection#with...()` methods.
 *
 * Method on `GridProjection` always returns an instance of type `GridProjection` even when
 * called on a sub-class. This trait's methods return the instance of the same type
 * it was called on.
 *
 * @author @java.author
 * @version @java.version
 */
trait ScalarTaskThreadContext[T <: GridProjection] extends ScalarConversions { this: PimpedType[T] =>
    /**
     * Properly typed version of `GridProjection#withName(...)` method.
     *
     * @param taskName Name of the task.
     */
    def withName$(@Nullable taskName: String): T =
        value.compute().withName(taskName).asInstanceOf[T]

    /**
     * Properly typed version of `GridProjection#withFailoverSpi(...)` method.
     *
     * @param spiName Name of the SPI.
     */
    def withFailoverSpi$(@Nullable spiName: String): T =
        value.compute().withFailoverSpi(spiName).asInstanceOf[T]

    /**
     * Properly typed version of `GridProjection#withTopologySpi(...)` method.
     *
     * @param spiName Name of the SPI.
     */
    def withTopologySpi$(@Nullable spiName: String): T =
        value.compute().withTopologySpi(spiName).asInstanceOf[T]

    /**
     * Properly typed version of `GridProjection#withCheckpointSpi(...)` method.
     *
     * @param spiName Name of the SPI.
     */
    def withCheckpointSpi$(@Nullable spiName: String): T =
        value.compute().withCheckpointSpi(spiName).asInstanceOf[T]

    /**
     * Properly typed version of `GridProjection#withLoadBalancingSpi(...)` method.
     *
     * @param spiName Name of the SPI.
     */
    def withLoadBalancingSpi$(@Nullable spiName: String): T =
        value.compute().withLoadBalancingSpi(spiName).asInstanceOf[T]

    /**
     * Properly typed version of `GridProjection#withResultClosure(...)` method.
     *
     * @param f Ad-hoc implementation of `GridComputeTask#result(...)` method.
     */
    def withResultClosure$(@Nullable f: (GridComputeJobResult, java.util.List[GridComputeJobResult]) => GridComputeJobResultPolicy): T =
        value.compute().withResultClosure(toClosure2X(f)).asInstanceOf[T]

}