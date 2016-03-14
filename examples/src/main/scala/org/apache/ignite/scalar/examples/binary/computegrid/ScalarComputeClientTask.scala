/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.apache.ignite.scalar.examples.binary.computegrid

import java.util.{ArrayList => JavaArrayList, Collection => JavaCollection, List => JavaList}

import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.compute.{ComputeJob, ComputeJobAdapter, ComputeJobResult, ComputeTaskSplitAdapter}
import org.apache.ignite.lang.IgniteBiTuple
import org.jetbrains.annotations.Nullable

import scala.collection.JavaConversions._

class ScalarComputeClientTask extends ComputeTaskSplitAdapter[JavaCollection[BinaryObject], Long] {
    /** @inheritdoc */
    protected def split(gridSize: Int, arg: JavaCollection[BinaryObject]): JavaCollection[_ <: ComputeJob] = {
        val jobs = new JavaArrayList[ScalarComputeClientJob]

        var employees = new JavaArrayList[BinaryObject]

        arg.foreach(employee => {
            employees.add(employee)

            if (employees.size == 3) {
                jobs.add(new ScalarComputeClientJob(employees))

                employees = new JavaArrayList[BinaryObject](3)
            }
        })

        if (!employees.isEmpty)
            jobs.add(new ScalarComputeClientJob(employees))

        jobs
    }

    /** @inheritdoc */
    @Nullable def reduce(results: JavaList[ComputeJobResult]): Long = {
        var sum = 0L
        var cnt = 0

        results.foreach(res => {
            val t = res.getData.asInstanceOf[IgniteBiTuple[Long, Int]]

            sum += t.get1
            cnt += t.get2
        })

        sum / cnt
    }
}

/**
  * Remote job for [[org.apache.ignite.scalar.examples.binary.computegrid.ScalarComputeClientTask]].
  */
private class ScalarComputeClientJob(arg: JavaCollection[BinaryObject]) extends ComputeJobAdapter {
    /** Collection of employees. */
    private final val employees = arg

    /** @inheritdoc */
    @Nullable def execute(): AnyRef = {
        var sum = 0L
        var cnt = 0

        employees.foreach(employee => {
            println(">>> Processing employee: " + employee.field("name"))

            val salary: Long = employee.field("salary")

            sum += salary
            cnt += 1
        })

        new IgniteBiTuple[Long, Int](sum, cnt)
    }
}
