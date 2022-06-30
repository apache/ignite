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

import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.check.Check
import io.gatling.core.check.CheckBuilder
import io.gatling.core.check.CheckMaterializer
import io.gatling.core.check.CountCriterionExtractor
import io.gatling.core.check.Extractor
import io.gatling.core.check.FindAllCriterionExtractor
import io.gatling.core.check.FindCriterionExtractor
import io.gatling.core.check.Preparer
import io.gatling.core.check.identityPreparer
import io.gatling.core.session.Expression
import io.gatling.core.session.ExpressionSuccessWrapper

/**
 * DSL for checks performed against the SQL query result set. Exposed as a `resetSet` check type.
 *
 * The result set is represented as a list of rows. Each row is a list of Any.
 *
 * Standard gatling extraction methods are supported: find, findAll, findRandom and count.
 */
trait IgniteSqlCheckSupport extends IgniteCheckSupport {
  /**
   * SQL query result check type.
   */
  trait SqlCheckType

  /**
   * Type of the SQL result set row.
   */
  type Row = List[Any]

  /**
   * Type of the raw Ignite SQL query operation result.
   */
  type RawSqlResult = List[Row]

  /**
   * Type of the prepared Ignite SQL query operation result.
   */
  type PreparedSqlResult = RawSqlResult

  /**
   * Check of Ignite SQL query operation result.
   */
  type SqlCheck = Check[RawSqlResult]

  /**
   * Implicit conversion from check builder to check itself.
   *
   * @param checkBuilder Check builder.
   * @param materializer Materializer.
   * @tparam T Check type.
   * @tparam P Prepared result type.
   * @return SqlCheck.
   */
  implicit def checkBuilder2SqlCheck[T, P](checkBuilder: CheckBuilder[T, P])(implicit
    materializer: CheckMaterializer[T, SqlCheck, RawSqlResult, P]
  ): SqlCheck =
    checkBuilder.build(materializer)

  /**
   * Implicit conversion to use the `exists` by default for Validate.
   *
   * @param validate Validate instance.
   * @param materializer Materializer.
   * @tparam T Check type.
   * @tparam P Type of the prepared result.
   * @tparam X Type of the extracted result.
   * @return SqlCheck.
   */
  implicit def validate2SqlCheck[T, P, X](validate: CheckBuilder.Validate[T, P, X])(implicit
    materializer: CheckMaterializer[T, SqlCheck, RawSqlResult, P]
  ): SqlCheck =
    validate.exists

  /**
   * Implicit conversion to use the `find(0).exists` by default for Find.
   *
   * @param find Find.
   * @param materializer Materializer.
   * @tparam T Check type.
   * @tparam P Type of the prepared result.
   * @tparam X Type of the extracted result.
   * @return SqlCheck.
   */
  implicit def find2SqlCheck[T, P, X](find: CheckBuilder.Find[T, P, X])(implicit
    materializer: CheckMaterializer[T, SqlCheck, RawSqlResult, P]
  ): SqlCheck =
    find.find.exists

  /**
   * Implicit materializer for preparation of raw result as input for SQL check.
   */
  implicit val SqlCheckMaterializer: CheckMaterializer[SqlCheckType, SqlCheck, RawSqlResult, PreparedSqlResult] =
    new CheckMaterializer[SqlCheckType, SqlCheck, RawSqlResult, PreparedSqlResult](identity) {
      override protected def preparer: Preparer[RawSqlResult, PreparedSqlResult] = identityPreparer
    }

  /**
   * Builder for the SQL query result check exposed as a `resultSet` DSL function.
   *
   * @return SQL check builder
   */
  def resultSet: CheckBuilder.MultipleFind.Default[SqlCheckType, PreparedSqlResult, Row] =
    new CheckBuilder.MultipleFind.Default[SqlCheckType, PreparedSqlResult, Row](displayActualValue = true) {
      override protected def findExtractor(occurrence: Int): Expression[Extractor[PreparedSqlResult, Row]] =
        new FindCriterionExtractor[PreparedSqlResult, Int, Row](
          "row",
          occurrence,
          occurrence,
          p => p.drop(occurrence).headOption.success
        ).expressionSuccess

      override protected def findAllExtractor: Expression[Extractor[PreparedSqlResult, Seq[Row]]] =
        new FindAllCriterionExtractor[PreparedSqlResult, Int, Row](
          "row",
          0,
          p => Some(p).success
        ).expressionSuccess

      override protected def countExtractor: Expression[Extractor[PreparedSqlResult, Int]] =
        new CountCriterionExtractor[PreparedSqlResult, Int](
          "row",
          0,
          p => Some(p.size).success
        ).expressionSuccess
    }
}
