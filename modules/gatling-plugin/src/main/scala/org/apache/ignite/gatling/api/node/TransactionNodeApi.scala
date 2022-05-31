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

package org.apache.ignite.gatling.api.node

import org.apache.ignite.gatling.api.TransactionApi
import org.apache.ignite.transactions.Transaction

case class TransactionNodeApi(wrapped: Transaction) extends TransactionApi {
  override def commit()(s: Unit => Unit, f: Throwable => Unit): Unit = {
    try {
      s(wrapped.commit())
    } catch  {
      case ex: Throwable => f(ex)
    } finally {
      wrapped.close()
    }
  }

  override def rollback()(s: Unit => Unit, f: Throwable => Unit): Unit = {
    try {
      s(wrapped.rollback())
    } catch {
      case ex: Throwable => f(ex)
    } finally {
      wrapped.close()
    }
  }
}
