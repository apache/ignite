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
package org.apache.ignite.gatling.testsuites

import org.apache.ignite.gatling.BinaryTest
import org.apache.ignite.gatling.CreateCacheTest
import org.apache.ignite.gatling.InvokeAllTest
import org.apache.ignite.gatling.InvokeTest
import org.apache.ignite.gatling.LockTest
import org.apache.ignite.gatling.ProtocolTest
import org.apache.ignite.gatling.PutAllGetAllTest
import org.apache.ignite.gatling.PutGetTest
import org.apache.ignite.gatling.SqlTest
import org.apache.ignite.gatling.TransactionTest
import org.junit.runner.RunWith
import org.junit.runners.Suite

/**
 * Ignite Gatling plugin tests.
 */
@RunWith(classOf[Suite])
@Suite.SuiteClasses(
  Array(
    classOf[BinaryTest],
    classOf[CreateCacheTest],
    classOf[InvokeTest],
    classOf[InvokeAllTest],
    classOf[LockTest],
    classOf[ProtocolTest],
    classOf[PutAllGetAllTest],
    classOf[PutGetTest],
    classOf[SqlTest],
    classOf[TransactionTest]
  )
)
class IgniteGatlingPluginTestSuite
