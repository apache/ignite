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

/**
 * Ignite Gatling DSL definitions.
 */
object Predef extends IgniteDsl {
  /** Fully replicated cache mode. */
  val REPLICATED = org.apache.ignite.cache.CacheMode.REPLICATED
  /** Partitioned cache mode */
  val PARTITIONED = org.apache.ignite.cache.CacheMode.PARTITIONED

  /** Transactional cache atomicity mode. */
  val TRANSACTIONAL = org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL
  /** Atomic cache atomicity mode. */
  val ATOMIC = org.apache.ignite.cache.CacheAtomicityMode.ATOMIC

  /** Pessimistic transaction concurrency control. */
  val PESSIMISTIC = org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC
  /** Optimistic transaction concurrency control. */
  val OPTIMISTIC = org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC

  /** Repeatable read transaction isolation level. */
  val REPEATABLE_READ = org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ
  /** Read committed transaction isolation level. */
  val READ_COMMITTED = org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED
  /** Serializable read transaction isolation level. */
  val SERIALIZABLE = org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE
}
