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
package org.apache.ignite.gatling.api.thin

import scala.util.Try

import org.apache.ignite.client.ClientTransaction
import org.apache.ignite.gatling.api.TransactionApi

/**
 * Implementation of TransactionApi working via the Ignite (thin) Client API.
 *
 * @param wrapped Enclosed IgniteClient instance.
 */
case class TransactionThinApi(wrapped: ClientTransaction) extends TransactionApi {
  /**
   * @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def commit()(s: Unit => Unit, f: Throwable => Unit): Unit =
    Try(wrapped.commit())
      .fold(f, s)

  /**
   * @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def rollback()(s: Unit => Unit, f: Throwable => Unit): Unit =
    Try(wrapped.rollback())
      .fold(f, s)

  /**
   * @inheritdoc
   * @param s @inheritdoc
   * @param f @inheritdoc
   */
  override def close()(s: Unit => Unit, f: Throwable => Unit): Unit =
    Try(wrapped.close())
      .fold(f, s)
}
