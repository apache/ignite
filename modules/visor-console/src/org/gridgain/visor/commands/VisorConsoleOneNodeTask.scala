/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands

import java.util.{Collections, Map => JavaMap, List => JavaList, UUID}
import scala.collection.JavaConversions._
import org.gridgain.grid.{GridEmptyProjectionException, GridNode}
import org.gridgain.grid.compute._
import org.gridgain.grid.kernal.GridEx
import org.gridgain.grid.resources.GridInstanceResource
import org.gridgain.grid.util.lang.{GridFunc => F}
import org.gridgain.grid.util.scala.impl

/**
 * Basic adapter for Visor tasks intended to query data from a single node.
 *
 * @tparam T Task argument type.
 * @tparam R Task result type.
 *
 */
trait VisorConsoleOneNodeTask[T <: VisorConsoleOneNodeArgument, R] extends GridComputeTask[T, R] {
    @impl def map(subgrid: JavaList[GridNode], arg: T): JavaMap[GridComputeJob, GridNode] =
        subgrid.find(_.id() == arg.nodeId) match {
            case Some(node) => Collections.singletonMap(
                new GridComputeJobAdapter() {
                    @GridInstanceResource
                    val g: GridEx = null

                    def execute(): Object = run(g, arg).asInstanceOf[Object]
                },
                node)

            case None => throw new GridEmptyProjectionException(
                "Target node to execute Visor job not found in grid [id=" + arg.nodeId + ", prj=" + subgrid + "]")
        }

    @impl def reduce(results: java.util.List[GridComputeJobResult]): R = {
        assert(results.size() == 1)

        reduce(F.first(results))
    }

    /**
     * Process job result.
     *
     * @param res Result to process
     * @return
     */
    protected def reduce(res: GridComputeJobResult): R = {
        if (res.getException == null)
            res.getData[R]
        else
            throw res.getException
    }

    @impl def result(res: GridComputeJobResult, rcvd: JavaList[GridComputeJobResult]): GridComputeJobResultPolicy = {
        // All Visor tasks should handle exceptions in reduce method.
        GridComputeJobResultPolicy.WAIT
    }

    /**
     * Execution logic of concrete task.
     *
     * @param g Local `Grid` instance.
     * @param arg Task argument.
     * @return Result.
     */
    protected def run(g: GridEx, arg: T): R
}

/**
 * Argument for a `VisorConsoleOneNodeTask` containing information on where task should run.
 */
trait VisorConsoleOneNodeArgument extends Serializable {
    /**
     * Id of the node where task should run.
     */
    val nodeId: UUID
}