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

import scala.annotation.implicitNotFound

import io.gatling.commons.validation.FailureWrapper
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.commons.validation.Validation
import io.gatling.core.check.Check
import io.gatling.core.check.Check.PreparedCache
import io.gatling.core.check.CheckBuilder
import io.gatling.core.check.CheckMaterializer
import io.gatling.core.check.CheckResult
import io.gatling.core.check.Extractor
import io.gatling.core.check.Preparer
import io.gatling.core.check.identityPreparer
import io.gatling.core.session.Expression
import io.gatling.core.session.ExpressionSuccessWrapper
import io.gatling.core.session.Session
import org.apache.ignite.gatling.IgniteCheck

class IgniteKeyValueCheckSupport {

  trait IgniteAllKeyValueCheckType

  type AllKeyValueResult[K, V] = Map[K, V]

  def simpleCheck[K, V](f: (AllKeyValueResult[K, V], Session) => Boolean): IgniteCheck[K, V] =
    Check.Simple(
      (response: AllKeyValueResult[K, V], session: Session, _: PreparedCache) =>
        if (f(response, session)) {
          CheckResult.NoopCheckResultSuccess
        } else {
          "Ignite check failed".failure
        },
      None
    )

  def simpleCheck[K, V](f: AllKeyValueResult[K, V] => Boolean): IgniteCheck[K, V] =
    Check.Simple(
      (response: AllKeyValueResult[K, V], _: Session, _: PreparedCache) =>
        if (f(response)) {
          CheckResult.NoopCheckResultSuccess
        } else {
          "Ignite check failed".failure
        },
      None
    )

  @implicitNotFound("Could not find a CheckMaterializer. This check might not be valid for Ignite.")
  implicit def checkBuilder2IgniteCheck[T, P, K, V](checkBuilder: CheckBuilder[T, P])
                                                   (implicit materializer:
                                                   CheckMaterializer[T, IgniteCheck[K, V], AllKeyValueResult[K, V], P]): IgniteCheck[K, V] =
    checkBuilder.build(materializer)

  @implicitNotFound("Could not find a CheckMaterializer. This check might not be valid for Ignite.")
  implicit def validate2IgniteCheck[T, P, X, K, V](validate: CheckBuilder.Validate[T, P, X])
                                                  (implicit materializer:
                                                  CheckMaterializer[T, IgniteCheck[K, V], AllKeyValueResult[K, V], P]): IgniteCheck[K, V] =
    validate.exists

  @implicitNotFound("Could not find a CheckMaterializer. This check might not be valid for Ignite.")
  implicit def find2IgniteCheck[T, P, X, K, V](find: CheckBuilder.Find[T, P, X])
                                              (implicit materializer:
                                              CheckMaterializer[T, IgniteCheck[K, V], AllKeyValueResult[K, V], P]): IgniteCheck[K, V] =
    find.find.exists

  class AllKeyValueCheckMaterializer[K, V] extends
    CheckMaterializer[IgniteAllKeyValueCheckType, IgniteCheck[K, V], AllKeyValueResult[K, V], AllKeyValueResult[K, V]](identity) {
    override protected def preparer: Preparer[AllKeyValueResult[K, V], AllKeyValueResult[K, V]] = identityPreparer
  }

  implicit def allKeyValueCheckMaterializer[K, V]: AllKeyValueCheckMaterializer[K, V] = new AllKeyValueCheckMaterializer[K, V]

  def allKeyValueExtractor[K, V]: Expression[Extractor[AllKeyValueResult[K, V], AllKeyValueResult[K, V]]] =
    new Extractor[AllKeyValueResult[K, V], AllKeyValueResult[K, V]] {
      override def name: String = "allRecords"

      override def apply(prepared: AllKeyValueResult[K, V]): Validation[Option[AllKeyValueResult[K, V]]] = Some(prepared).success

      override def arity: String = "find"
    }.expressionSuccess

  private def allKeyValueResults[K, V] =
    new CheckBuilder.Find.Default[IgniteAllKeyValueCheckType, AllKeyValueResult[K, V], AllKeyValueResult[K, V]](
      allKeyValueExtractor,
      displayActualValue = true
    )

  def allResults[K, V]: CheckBuilder.Find.Default[IgniteAllKeyValueCheckType, AllKeyValueResult[K, V], AllKeyValueResult[K, V]] =
    allKeyValueResults
}
