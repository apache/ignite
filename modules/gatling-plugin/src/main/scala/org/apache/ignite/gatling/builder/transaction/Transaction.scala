package org.apache.ignite.gatling.builder.transaction

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.action.ignite.{TransactionCommitAction, TransactionRollbackAction, TransactionStartAction}
import org.apache.ignite.gatling.builder.IgniteActionBuilder
import org.apache.ignite.transactions.{TransactionConcurrency, TransactionIsolation}

trait TransactionSupport {
  def requestName: Expression[String]

  def tx(concurrency: TransactionConcurrency, isolation: TransactionIsolation): TransactionStartBuilderTimeoutStep =
    TransactionStartBuilderTimeoutStep(requestName)
  def tx: TransactionStartBuilder = TransactionStartBuilder(requestName)

  def commit: TransactionCommitActionBuilder = TransactionCommitActionBuilder(requestName)
  def rollback: TransactionRollbackActionBuilder = TransactionRollbackActionBuilder(requestName)
}

case class TransactionStartBuilderTimeoutStep(requestName: Expression[String]) extends IgniteActionBuilder {
  def timeout(timeout: Long): TransactionStartBuilderTimeoutStep = TransactionStartBuilderTimeoutStep(requestName)
  def txSize(txSize: Int): TransactionStartBuilderTimeoutStep = TransactionStartBuilderTimeoutStep(requestName)

  override def build(ctx: ScenarioContext, next: Action): Action =
    TransactionCommitAction(requestName, next, ctx)
}

case class TransactionStartBuilder(requestName: Expression[String]) extends IgniteActionBuilder {
  override def build(ctx: ScenarioContext, next: Action): Action =
    TransactionStartAction(requestName, next, ctx)
}

case class TransactionParameters(concurrency: TransactionConcurrency, isolation: TransactionIsolation,
                                 timeout: Long, txSize: Int)

case class TransactionCommitActionBuilder(requestName: Expression[String]) extends IgniteActionBuilder {
  override def build(ctx: ScenarioContext, next: Action): TransactionCommitAction =
    TransactionCommitAction(requestName, next, ctx)
}

case class TransactionRollbackActionBuilder(requestName: Expression[String]) extends IgniteActionBuilder {
  override def build(ctx: ScenarioContext, next: Action): TransactionRollbackAction =
    TransactionRollbackAction(requestName, next, ctx)
}
