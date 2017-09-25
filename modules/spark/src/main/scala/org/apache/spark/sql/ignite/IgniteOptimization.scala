package org.apache.spark.sql.ignite

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * @author NIzhikov
  */
object IgniteOptimization extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan) = {
        println("IgniteOptimization - " + plan)
        plan
    }
}
