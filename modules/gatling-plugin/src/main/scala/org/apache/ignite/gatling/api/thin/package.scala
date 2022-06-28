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

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

package object thin {
  /** Execution context to run callbacks. */
  implicit val Ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  /**
   * Helper to execute future with the callback functions passed.
   */
  trait CompletionSupport {
    /**
     * @param fut Future to execute.
     * @param s Function to be called if future is competed successfully.
     * @param f Function to be called if exception occurs.
     * @tparam T Type of the future result.
     */
    def withCompletion[T](fut: Future[T])(s: T => Unit, f: Throwable => Unit): Unit = fut.onComplete {
      case Success(value)     => s(value)
      case Failure(exception) => f(exception)
    }
  }
}
