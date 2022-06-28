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
import org.apache.ignite.gatling.SqlCheck

trait IgniteSqlCheckSupport {

  trait IgniteAllSqlCheckType

  type AllSqlResult = List[List[Any]]

  def simpleSqlCheck(f: (AllSqlResult, Session) => Boolean): SqlCheck =
    Check.Simple(
      (response: AllSqlResult, session: Session, _: PreparedCache) =>
        if (f(response, session)) {
          CheckResult.NoopCheckResultSuccess
        } else {
          "Sql check failed".failure
        },
      None
    )

  def simpleSqlCheck(f: AllSqlResult => Boolean): SqlCheck =
    Check.Simple(
      (response: AllSqlResult, _: Session, _: PreparedCache) =>
        if (f(response)) {
          CheckResult.NoopCheckResultSuccess
        } else {
          "Sql check failed".failure
        },
      None
    )

  @implicitNotFound("Could not find a CheckMaterializer. This check might not be valid for Ignite.")
  implicit def checkBuilder2SqlCheck[T, P](checkBuilder: CheckBuilder[T, P])(implicit
    materializer: CheckMaterializer[T, SqlCheck, AllSqlResult, P]
  ): SqlCheck =
    checkBuilder.build(materializer)

  @implicitNotFound("Could not find a CheckMaterializer. This check might not be valid for Ignite.")
  implicit def validate2SqlCheck[T, P, X](validate: CheckBuilder.Validate[T, P, X])(implicit
    materializer: CheckMaterializer[T, SqlCheck, AllSqlResult, P]
  ): SqlCheck =
    validate.exists

  @implicitNotFound("Could not find a CheckMaterializer. This check might not be valid for Ignite.")
  implicit def find2SqlCheck[T, P, X](find: CheckBuilder.Find[T, P, X])(implicit
    materializer: CheckMaterializer[T, SqlCheck, AllSqlResult, P]
  ): SqlCheck =
    find.find.exists

  class AllSqlCheckMaterializer extends CheckMaterializer[IgniteAllSqlCheckType, SqlCheck, AllSqlResult, AllSqlResult](identity) {
    override protected def preparer: Preparer[AllSqlResult, AllSqlResult] = identityPreparer
  }

  implicit val AllSqlCheckMaterializer: AllSqlCheckMaterializer = new AllSqlCheckMaterializer

  val AllSqlExtractor: Expression[Extractor[AllSqlResult, AllSqlResult]] =
    new Extractor[AllSqlResult, AllSqlResult] {
      override def name: String = "allRecords"

      override def apply(prepared: AllSqlResult): Validation[Option[AllSqlResult]] = Some(prepared).success

      override def arity: String = "find"
    }.expressionSuccess

  val AllSqlResults =
    new CheckBuilder.Find.Default[IgniteAllSqlCheckType, AllSqlResult, AllSqlResult](
      AllSqlExtractor,
      displayActualValue = true
    )

  val allSqlResults: CheckBuilder.Find.Default[IgniteAllSqlCheckType, AllSqlResult, AllSqlResult] =
    AllSqlResults
}
