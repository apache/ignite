package org.apache.ignite.gatling.builder.cache

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.SqlCheck
import org.apache.ignite.gatling.action.cache
import org.apache.ignite.gatling.builder.IgniteActionBuilder

case class CacheSqlActionBuilder(requestName: Expression[String],
                                 cacheName: Expression[String],
                                 sql: Expression[String],
                                 argsList: List[Expression[Any]] = List.empty,
                                 partitionsList: Expression[List[Int]] = _ => List.empty.success,
                                 checks: Seq[SqlCheck] = Seq.empty) extends IgniteActionBuilder {

  def check(newChecks: SqlCheck*): CacheSqlActionBuilder = this.copy(checks = newChecks)
  def args(newArgs: Expression[Any]*): CacheSqlActionBuilder = this.copy(argsList = newArgs.toList)
  def partitions(newPartitions: Expression[List[Int]]): CacheSqlActionBuilder = this.copy(partitionsList = newPartitions)

  override def build(ctx: ScenarioContext, next: Action): Action = cache.CacheSqlAction(
    requestName, cacheName, sql, argsList, partitionsList, checks, next, ctx)
}
