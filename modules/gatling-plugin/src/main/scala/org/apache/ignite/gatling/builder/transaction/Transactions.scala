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
import org.apache.ignite.gatling.action.ignite.TransactionCommitAction
import org.apache.ignite.gatling.action.ignite.TransactionEndAction
import org.apache.ignite.gatling.action.ignite.TransactionRollbackAction
import org.apache.ignite.gatling.action.ignite.TransactionStartAction
import org.apache.ignite.gatling.builder.IgniteActionBuilder
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation

trait Transactions {
  def tx(requestName: Expression[String]): TransactionBuilderBase = TransactionBuilderBase(requestName)
  def tx: TransactionBuilderBase = TransactionBuilderBase(EmptyStringExpressionSuccess)

  def commit: TransactionCommitActionBuilder = TransactionCommitActionBuilder()

  def rollback: TransactionRollbackActionBuilder = TransactionRollbackActionBuilder()
}

case class TransactionBuilderBase(requestName: Expression[String]) {
  def apply(concurrency: TransactionConcurrency, isolation: TransactionIsolation): TransactionBuilderTimeoutStep =
    TransactionBuilderTimeoutStep(requestName,
      TransactionParameters(concurrency = Some(concurrency), isolation = Some(isolation)))

  def apply(transactionChain: ChainBuilder*): ChainBuilder =
    exec(TransactionStartBuilder(requestName, TransactionParameters()))
      .exec(transactionChain)
      .exec(TransactionEndBuilder(requestName))
}

case class TransactionBuilderTimeoutStep(requestName: Expression[String],
                                         params: TransactionParameters) {
  def timeout(timeout: Expression[Long]): TransactionBuilderTimeoutStep =
    TransactionBuilderTimeoutStep(requestName, params.copy(timeout = Some(timeout)))

  def txSize(txSize: Expression[Int]): TransactionBuilderTimeoutStep =
    TransactionBuilderTimeoutStep(requestName, params.copy(txSize = Some(txSize)))

  def apply(transactionChain: ChainBuilder*): ChainBuilder =
    exec(TransactionStartBuilder(requestName, params))
      .exec(transactionChain)
      .exec(TransactionEndBuilder(requestName))
}

case class TransactionParameters(concurrency: Option[TransactionConcurrency] = None,
                                 isolation: Option[TransactionIsolation] = None,
                                 timeout: Option[Expression[Long]] = None,
                                 txSize: Option[Expression[Int]] = None)

case class TransactionCommitActionBuilder(requestName: Expression[String] = EmptyStringExpressionSuccess) extends IgniteActionBuilder {
  def as(requestName: Expression[String]): ActionBuilder = this.copy(requestName=requestName)

  override def build(ctx: ScenarioContext, next: Action): TransactionCommitAction =
    TransactionCommitAction(requestName, next, ctx)
}

case class TransactionRollbackActionBuilder(requestName: Expression[String] = EmptyStringExpressionSuccess) extends IgniteActionBuilder {
  def as(requestName: Expression[String]): ActionBuilder = this.copy(requestName=requestName)

  override def build(ctx: ScenarioContext, next: Action): TransactionRollbackAction =
    TransactionRollbackAction(requestName, next, ctx)
}

case class TransactionStartBuilder(requestName: Expression[String],
                                   params: TransactionParameters) extends IgniteActionBuilder {
  override def build(ctx: ScenarioContext, next: Action): Action =
    TransactionStartAction(requestName, TransactionParameters(), next, ctx)
}

case class TransactionEndBuilder(requestName: Expression[String]) extends IgniteActionBuilder {
  override def build(ctx: ScenarioContext, next: Action): Action =
    TransactionEndAction(requestName, next, ctx)
}
