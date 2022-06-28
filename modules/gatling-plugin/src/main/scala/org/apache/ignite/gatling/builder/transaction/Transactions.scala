/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.gatling.builder.transaction

import io.gatling.core.Predef.exec
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.EmptyStringExpressionSuccess
import io.gatling.core.session.Expression
import io.gatling.core.structure.ChainBuilder
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.action.ignite.TransactionCloseAction
import org.apache.ignite.gatling.action.ignite.TransactionCommitAction
import org.apache.ignite.gatling.action.ignite.TransactionRollbackAction
import org.apache.ignite.gatling.action.ignite.TransactionStartAction
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation

/**
 * DSL to create transaction operations.
 */
trait Transactions {
  /**
   * Start constructing of the transaction with the provided request name.
   *
   * @param requestName Request name.
   * @return TransactionBuilder.
   */
  def tx(requestName: Expression[String]): TransactionBuilder = TransactionBuilder(requestName)

  /**
   * Start constructing of the transaction with the default request name.
   *
   * @return TransactionBuilder.
   */
  def tx: TransactionBuilder = TransactionBuilder(EmptyStringExpressionSuccess)

  /**
   * Create transaction commit request with the default request name.
   *
   * @return TransactionCommitActionBuilder.
   */
  def commit: TransactionCommitActionBuilder = TransactionCommitActionBuilder()

  /**
   * Create transaction commit request with the default request name.
   *
   * @return TransactionRollbackActionBuilder.
   */
  def rollback: TransactionRollbackActionBuilder = TransactionRollbackActionBuilder()
}

/**
 * Transaction parameters.
 *
 * @param concurrency Concurrency.
 * @param isolation Isolation.
 * @param timeout Timeout in milliseconds.
 * @param txSize Number of entries participating in transaction (may be approximate).
 */
case class TransactionParameters(
  concurrency: Option[TransactionConcurrency] = None,
  isolation: Option[TransactionIsolation] = None,
  timeout: Option[Expression[Long]] = None,
  txSize: Option[Expression[Int]] = None
)

/**
 * Transaction builder.
 *
 * @param requestName Request name.
 */
case class TransactionBuilder(requestName: Expression[String]) {
  /**
   * Specify concurrency and isolation for the transaction.
   *
   * @param concurrency Concurrency.
   * @param isolation Isolation.
   * @return Builder to specify timeout and transaction size.
   */
  def apply(concurrency: TransactionConcurrency, isolation: TransactionIsolation): TransactionBuilderTimeoutStep =
    TransactionBuilderTimeoutStep(requestName, TransactionParameters(concurrency = Some(concurrency), isolation = Some(isolation)))

  /**
   * Builds full chain of actions that make up a transaction.
   *
   * Full chain consists of:
   *  - transaction start action
   *  - chain of user-provided actions
   *  - close transaction action.
   *
   * @param transactionChain Chain of user actions to be executed within a transaction.
   * @return Full chain of actions.
   */
  def apply(transactionChain: ChainBuilder*): ChainBuilder =
    exec(TransactionStartActionBuilder(requestName, TransactionParameters()))
      .exec(transactionChain)
      .exec(TransactionCloseActionBuilder(requestName))
}

/**
 * Transaction builder: step for timeout and txSize parameters.
 *
 * @param requestName Request name.
 * @param params Transaction parameters collected so far.
 */
case class TransactionBuilderTimeoutStep(requestName: Expression[String], params: TransactionParameters) {
  /**
   * Specify transaction timeout.
   *
   * @param timeout Timeout value in milliseconds.
   * @return itself.
   */
  def timeout(timeout: Expression[Long]): TransactionBuilderTimeoutStep =
    TransactionBuilderTimeoutStep(requestName, params.copy(timeout = Some(timeout)))

  /**
   * Specify transaction size.
   *
   * @param txSize Number of entries participating in transaction (may be approximate).
   * @return itself.
   */
  def txSize(txSize: Expression[Int]): TransactionBuilderTimeoutStep =
    TransactionBuilderTimeoutStep(requestName, params.copy(txSize = Some(txSize)))

  /**
   * Builds full chain of actions that make up a transaction.
   *
   * Full chain consists of:
   *  - transaction start action
   *  - chain of user-provided actions
   *  - close transaction action.
   *
   * @param transactionChain Chain of user actions to be executed within a transaction.
   * @return Full chain of actions.
   */
  def apply(transactionChain: ChainBuilder*): ChainBuilder =
    exec(TransactionStartActionBuilder(requestName, params))
      .exec(transactionChain)
      .exec(TransactionCloseActionBuilder(requestName))
}

/**
 * Transaction commit action builder.
 *
 * @param requestName Request name.
 */
case class TransactionCommitActionBuilder(requestName: Expression[String] = EmptyStringExpressionSuccess) extends ActionBuilder {
  /**
   * Specify request name for action.
   *
   * @param requestName Request name.
   * @return itself.
   */
  def as(requestName: Expression[String]): ActionBuilder = this.copy(requestName = requestName)

  /**
   * Builds an action.
   *
   * @param ctx The scenario context.
   * @param next The action that will be chained with the Action build by this builder.
   * @return The resulting action.
   */
  def build(ctx: ScenarioContext, next: Action): Action =
    new TransactionCommitAction(requestName, next, ctx)
}

/**
 * Transaction rollback action builder.
 *
 * @param requestName Request name.
 */
case class TransactionRollbackActionBuilder(requestName: Expression[String] = EmptyStringExpressionSuccess) extends ActionBuilder {
  /**
   * Specify request name for action.
   *
   * @param requestName Request name.
   * @return itself.
   */
  def as(requestName: Expression[String]): ActionBuilder = this.copy(requestName = requestName)

  /**
   * Builds an action.
   *
   * @param ctx The scenario context.
   * @param next The action that will be chained with the Action build by this builder.
   * @return The resulting action.
   */
  override def build(ctx: ScenarioContext, next: Action): TransactionRollbackAction =
    new TransactionRollbackAction(requestName, next, ctx)
}

/**
 * Transaction start action builder.
 *
 * @param requestName Request name.
 * @param params Transaction parameters.
 */
case class TransactionStartActionBuilder(requestName: Expression[String], params: TransactionParameters) extends ActionBuilder {
  /**
   * Builds an action.
   *
   * @param ctx The scenario context.
   * @param next The action that will be chained with the Action build by this builder.
   * @return The resulting action.
   */
  override def build(ctx: ScenarioContext, next: Action): Action =
    new TransactionStartAction(requestName, params, next, ctx)
}

/**
 * Transaction close action builder.
 *
 * @param requestName Request name.
 */
case class TransactionCloseActionBuilder(requestName: Expression[String]) extends ActionBuilder {
  /**
   * Builds an action.
   *
   * @param ctx The scenario context.
   * @param next The action that will be chained with the Action build by this builder.
   * @return The resulting action.
   */
  override def build(ctx: ScenarioContext, next: Action): Action =
    new TransactionCloseAction(requestName, next, ctx)
}
