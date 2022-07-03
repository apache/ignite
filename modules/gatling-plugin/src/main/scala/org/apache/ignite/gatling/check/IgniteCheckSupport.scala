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
package org.apache.ignite.gatling.check

import io.gatling.commons.validation.FailureWrapper
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.commons.validation.Validation
import io.gatling.core.Predef.Session
import io.gatling.core.check.Validator
import io.gatling.core.session.Expression

/**
 * Check validation functions common both for results obtained both from Ignite Key-Value and SQL APIs.
 */
trait IgniteCheckSupport {
  /**
   * Implicit conversion from lambda to Validator to support validation as:
   * {{{
   *  ignite(
   *    sql(cache, "SELECT id, name FROM City WHERE id = ?").args("#{id}")
   *     check(
   *       resultSet.findALl.validate((rows: Seq[Row], s: Session) => row.head(1) == "Nsk")
   *     )
   *  )
   * }}}
   *
   * @param predicate Predicate validationg the result in context of the session.
   * @tparam P Type of the prepared operation result.
   * @return PredicateValidator.
   */
  implicit def predicateToValidator[P](predicate: (P, Session) => Boolean): Expression[Validator[P]] = s =>
    new PredicateValidator[P](predicate, s).success

  /**
   * Validator calling the provided predicate function to validate the operation result.
   *
   * @param predicate Predicate lambda.
   * @param session Session.
   * @tparam P Type of the prepared operation result.
   */
  final class PredicateValidator[P](predicate: (P, Session) => Boolean, session: Session) extends Validator[P] {
    /** Name of the validator. */
    val name = "predicate"

    /**
     * Applies the predicate to validate the actual result.
     *
     * @param actual Actual result of operation.
     * @param displayActualValue Display actual value.
     * @return Validation instance.
     */
    def apply(actual: Option[P], displayActualValue: Boolean): Validation[Option[P]] =
      if (predicate(actual.get, session)) {
        actual.success
      } else {
        "check failed".failure
      }
  }
}
