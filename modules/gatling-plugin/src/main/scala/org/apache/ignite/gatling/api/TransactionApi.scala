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
package org.apache.ignite.gatling.api

/**
 * Wrapper around the Ignite org.apache.ignite.transactions.Transaction object.
 */
trait TransactionApi {
  /**
   * Commits the enclosed transaction
   *
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def commit()(s: Unit => Unit, f: Throwable => Unit): Unit

  /**
   * Rollbacks the enclosed transaction
   *
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def rollback()(s: Unit => Unit, f: Throwable => Unit): Unit

  /**
   * Closes the enclosed transaction
   *
   * @param s Function to be called if operation is competed successfully.
   * @param f Function to be called if exception occurs.
   */
  def close()(s: Unit => Unit, f: Throwable => Unit): Unit
}
