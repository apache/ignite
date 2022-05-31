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

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.action.ignite.{TransactionCommitAction, TransactionRollbackAction, TransactionStartAction}
import org.apache.ignite.gatling.builder.IgniteActionBuilder
import org.apache.ignite.transactions.{TransactionConcurrency, TransactionIsolation}

trait TransactionSupport {
  def requestName: Expression[String]

  def txStart(concurrency: TransactionConcurrency, isolation: TransactionIsolation): TransactionStartBuilderTimeoutStep =
    TransactionStartBuilderTimeoutStep(requestName,
      TransactionParameters(concurrency = Some(concurrency), isolation = Some(isolation)))
  def txStart: TransactionStartBuilder = TransactionStartBuilder(requestName, TransactionParameters())

  def commit: TransactionCommitActionBuilder = TransactionCommitActionBuilder(requestName)
  def rollback: TransactionRollbackActionBuilder = TransactionRollbackActionBuilder(requestName)
}

case class TransactionStartBuilderTimeoutStep(requestName: Expression[String],
                                              params: TransactionParameters) extends IgniteActionBuilder {
  def timeout(timeout: Expression[Long]): TransactionStartBuilderTimeoutStep =
    TransactionStartBuilderTimeoutStep(requestName, params.copy(timeout = Some(timeout)))
  def txSize(txSize: Expression[Int]): TransactionStartBuilderTimeoutStep =
    TransactionStartBuilderTimeoutStep(requestName, params.copy(txSize = Some(txSize)))

  override def build(ctx: ScenarioContext, next: Action): Action =
    TransactionStartAction(requestName, params, next, ctx)
}

case class TransactionStartBuilder(requestName: Expression[String],
                                              params: TransactionParameters) extends IgniteActionBuilder {
  override def build(ctx: ScenarioContext, next: Action): Action =
    TransactionStartAction(requestName, TransactionParameters(), next, ctx)
}

case class TransactionParameters(concurrency: Option[TransactionConcurrency] = None,
                                 isolation: Option[TransactionIsolation] = None,
                                 timeout: Option[Expression[Long]] = None,
                                 txSize: Option[Expression[Int]] = None)

case class TransactionCommitActionBuilder(requestName: Expression[String]) extends IgniteActionBuilder {
  override def build(ctx: ScenarioContext, next: Action): TransactionCommitAction =
    TransactionCommitAction(requestName, next, ctx)
}

case class TransactionRollbackActionBuilder(requestName: Expression[String]) extends IgniteActionBuilder {
  override def build(ctx: ScenarioContext, next: Action): TransactionRollbackAction =
    TransactionRollbackAction(requestName, next, ctx)
}
