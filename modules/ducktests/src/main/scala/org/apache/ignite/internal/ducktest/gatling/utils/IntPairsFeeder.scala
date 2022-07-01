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
package org.apache.ignite.internal.ducktest.gatling.utils

import java.util.concurrent.atomic.AtomicInteger

import io.gatling.core.feeder.Feeder
import io.gatling.core.feeder.Record

/**
 * Helper feeder class generating long key and long value pairs.
 */
class IntPairsFeeder extends Feeder[Int] {
  private val atomicInteger: AtomicInteger = new AtomicInteger(0)

  /**
   * @inheritdoc
   * @return @inheritdoc
   */
  override def hasNext: Boolean = true

  /**
   * @inheritdoc
   * @return @inheritdoc
   */
  override def next(): Record[Int] =
    Map("key" -> atomicInteger.incrementAndGet(), "value" -> atomicInteger.incrementAndGet())
}
