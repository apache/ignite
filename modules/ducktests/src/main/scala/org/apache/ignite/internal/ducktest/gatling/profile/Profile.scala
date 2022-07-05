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
package org.apache.ignite.internal.ducktest.gatling.profile

import io.gatling.core.structure.ScenarioBuilder

/**
 * Load testing profile.
 *
 * Collection of scenarios for caches creation, test data preload and load testing.
 */
trait Profile {
  /** @return Scenario for caches creation. */
  def ddl: ScenarioBuilder

  /** @return Scenario for test data pre-load. */
  def data: (Int, ScenarioBuilder)

  /**
   * Collection of load test scenarios to execute in parallel.
   *
   * For each scenario, the relative frequency is indicated in percentages. Say, if the
   * profile has 80% reads and 20% writes it may may represented as:
   *
   * {{{
   *   override def test = Map(
   *     20.0 -> writeScenario,
   *     80.0 -> readScenario
   *   )
   * }}}
   *
   * Sum of percentages shouldn't exceed 100.0
   *
   * @return Map from proportion to scenario.
   */
  def test: Map[Double, ScenarioBuilder]
}
