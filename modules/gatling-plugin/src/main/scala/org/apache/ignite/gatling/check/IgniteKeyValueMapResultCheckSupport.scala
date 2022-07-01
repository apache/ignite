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
import io.gatling.commons.validation.Validation
import io.gatling.core.check.CheckBuilder
import io.gatling.core.check.CheckMaterializer
import io.gatling.core.check.Extractor
import io.gatling.core.check.Preparer
import io.gatling.core.check.identityPreparer
import io.gatling.core.session.Expression
import io.gatling.core.session.ExpressionSuccessWrapper

/**
 * Support checks for the Ignite key-value operations results.
 */
trait IgniteKeyValueMapResultCheckSupport extends IgniteKeyValueCheckSupport {
  /**
   * Check of Key-Value operation result represented as a map.
   */
  trait IgniteMapResultCheckType

  /**
   * Type of the Ignite key-value operation result.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   */
  type MapResult[K, V] = RawResult[K, V]

  /**
   * Materializer for Ignite key-value result check.
   *
   * Does nothing in fact - just return the raw operation result.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   */
  class MapResultCheckMaterializer[K, V]
      extends CheckMaterializer[IgniteMapResultCheckType, IgniteCheck[K, V], MapResult[K, V], MapResult[K, V]](identity) {
    /**
     * Transform the raw response into something that will be used as check input.
     * @return No-op preparer.
     */
    override protected def preparer: Preparer[MapResult[K, V], MapResult[K, V]] = identityPreparer
  }

  /**
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   * @return Implicit materiolizer for Ignite key-value result check.
   */
  implicit def mapResultCheckMaterializer[K, V]: MapResultCheckMaterializer[K, V] = new MapResultCheckMaterializer[K, V]

  /**
   * AllKeyValueResult extractor - does nothing - just return the prepared result.
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   * @return Extractor.
   */
  def mapResultExtractor[K, V]: Expression[Extractor[MapResult[K, V], MapResult[K, V]]] =
    new Extractor[MapResult[K, V], MapResult[K, V]] {
      override val name: String = "map"

      override def apply(prepared: MapResult[K, V]): Validation[Option[MapResult[K, V]]] = Some(prepared).success

      override val arity: String = "find"
    }.expressionSuccess

  /**
   * Builder for the SQL query result check exposed as a `mapResult` DSL function.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   * @return Check builder.
   */
  def mapResult[K, V]: CheckBuilder.Find.Default[IgniteMapResultCheckType, MapResult[K, V], MapResult[K, V]] =
    new CheckBuilder.Find.Default[IgniteMapResultCheckType, MapResult[K, V], MapResult[K, V]](
      mapResultExtractor,
      displayActualValue = true
    )
}
