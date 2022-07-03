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
import io.gatling.core.check.CheckBuilder
import io.gatling.core.check.CheckMaterializer
import io.gatling.core.check.CountCriterionExtractor
import io.gatling.core.check.Extractor
import io.gatling.core.check.FindAllCriterionExtractor
import io.gatling.core.check.FindCriterionExtractor
import io.gatling.core.check.Preparer
import io.gatling.core.session.Expression
import io.gatling.core.session.ExpressionSuccessWrapper

/**
 * Support checks for the Ignite key-value operations results.
 */
trait IgniteKeyValueEntriesCheckSupport extends IgniteKeyValueCheckSupport {
  /**
   * Check of Key-Value operation result represented as a list of entries.
   */
  trait EntriesCheckType

  /**
   * Type of key-value operation entry.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   * @param key Key.
   * @param value Result.
   */
  case class Entry[K, V](key: K, value: V)

  /**
   * Type of the ignite key-value operation result prepared for check input.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   */
  type EntriesResult[K, V] = Seq[Entry[K, V]]

  /**
   * Materializer for Ignite key-value result check.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   */
  class EntriesCheckMaterializer[K, V]
      extends CheckMaterializer[EntriesCheckType, IgniteCheck[K, V], RawResult[K, V], EntriesResult[K, V]](specializer = identity) {
    /**
     * Transform the raw response into something that will be used as check input.
     * @return No-op preparer.
     */
    override protected def preparer: Preparer[RawResult[K, V], EntriesResult[K, V]] = _.filter { case (_, v) =>
      v != null
    }.toList.map(e => Entry(e._1, e._2)).success
  }

  /**
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   * @return Implicit materiolizer for Ignite key-value result check.
   */
  implicit def entriesCheckMaterializer[K, V]: EntriesCheckMaterializer[K, V] = new EntriesCheckMaterializer[K, V]

  /**
   * Builder for the SQL query result check exposed as a `entries` DSL function.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   * @return Ignite check builder.
   */
  def entries[K, V]: CheckBuilder.MultipleFind.Default[EntriesCheckType, EntriesResult[K, V], Entry[K, V]] =
    new CheckBuilder.MultipleFind.Default[EntriesCheckType, EntriesResult[K, V], Entry[K, V]](displayActualValue = true) {

      override protected def findExtractor(occurrence: Int): Expression[Extractor[EntriesResult[K, V], Entry[K, V]]] =
        new FindCriterionExtractor[EntriesResult[K, V], Int, Entry[K, V]](
          "entry",
          occurrence,
          occurrence,
          p => p.drop(occurrence).headOption.success
        ).expressionSuccess

      override protected def findAllExtractor: Expression[Extractor[EntriesResult[K, V], Seq[Entry[K, V]]]] =
        new FindAllCriterionExtractor[EntriesResult[K, V], Int, Entry[K, V]](
          "entry",
          0,
          p => Some(p).success
        ).expressionSuccess

      override protected def countExtractor: Expression[Extractor[EntriesResult[K, V], Int]] =
        new CountCriterionExtractor[EntriesResult[K, V], Int](
          "entry",
          0,
          p => Some(p.size).success
        ).expressionSuccess
    }
}
