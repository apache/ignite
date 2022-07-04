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

import io.gatling.core.check.Check
import io.gatling.core.check.CheckBuilder
import io.gatling.core.check.CheckMaterializer

/**
 * Base check validation functions for results obtained from the Ignite Key-Value API.
 */
trait IgniteKeyValueCheckSupport extends IgniteCheckSupport {
  /**
   * Type of the raw ignite key-value operation result.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   */
  type RawResult[K, V] = Map[K, V]

  /**
   * Alias for Ignite key-value operation result check type.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   */
  type IgniteCheck[K, V] = Check[RawResult[K, V]]

  /**
   * Implicit conversion from check builder to check itself.
   *
   * @param checkBuilder Check builder.
   * @param materializer Materializer.
   * @tparam T Check type.
   * @tparam P Prepared result type.
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   * @return IgniteCheck.
   */
  implicit def checkBuilder2IgniteCheck[T, P, K, V](checkBuilder: CheckBuilder[T, P])(implicit
    materializer: CheckMaterializer[T, IgniteCheck[K, V], RawResult[K, V], P]
  ): IgniteCheck[K, V] =
    checkBuilder.build(materializer)

  /**
   * Implicit conversion to use the `exists` by default for Validate.
   *
   * @param validate Check valiedate.
   * @param materializer Materializer.
   * @tparam T Check type.
   * @tparam P Type of the prepared result.
   * @tparam X Type of the extracted result.
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   * @return IgniteCheck.
   */
  implicit def validate2IgniteCheck[T, P, X, K, V](validate: CheckBuilder.Validate[T, P, X])(implicit
    materializer: CheckMaterializer[T, IgniteCheck[K, V], RawResult[K, V], P]
  ): IgniteCheck[K, V] =
    validate.exists

  /**
   * Implicit conversion to use the `find(0).exists` by default for Find.
   *
   * @param find Find.
   * @param materializer Materializer.
   * @tparam T Check type.
   * @tparam P Type of the prepared result.
   * @tparam X Type of the extracted result.
   * @tparam K Type of the cache key.
   * @tparam V Type of the operation result.
   * @return IgniteCheck.
   */
  implicit def find2IgniteCheck[T, P, X, K, V](find: CheckBuilder.Find[T, P, X])(implicit
    materializer: CheckMaterializer[T, IgniteCheck[K, V], RawResult[K, V], P]
  ): IgniteCheck[K, V] =
    find.find.exists
}
