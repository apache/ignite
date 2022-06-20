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

package org.apache.ignite.gatling

object Predef extends IgniteDsl {
  val REPLICATED = org.apache.ignite.cache.CacheMode.REPLICATED
  val PARTITIONED = org.apache.ignite.cache.CacheMode.PARTITIONED

  val TRANSACTIONAL = org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL
  val ATOMIC = org.apache.ignite.cache.CacheAtomicityMode.ATOMIC

  val PESSIMISTIC = org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC
  val OPTIMISTIC = org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC

  val REPEATABLE_READ = org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ
  val READ_COMMITTED = org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED
  val SERIALIZABLE = org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE
}
