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

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.rest.RestProcessorHangTest;
import org.apache.ignite.internal.processors.rest.RestProcessorInitializationTest;
import org.apache.ignite.internal.processors.rest.RestProtocolStartTest;
import org.apache.ignite.internal.processors.rest.handlers.cache.GridCacheAtomicCommandHandlerSelfTest;
import org.apache.ignite.internal.processors.rest.handlers.cache.GridCacheCommandHandlerSelfTest;
import org.apache.ignite.internal.processors.rest.handlers.cache.GridCacheMetadataCommandTest;
import org.apache.ignite.internal.processors.rest.handlers.log.GridLogCommandHandlerTest;
import org.apache.ignite.internal.processors.rest.handlers.query.GridQueryCommandHandlerTest;
import org.apache.ignite.internal.processors.rest.handlers.top.CacheTopologyCommandHandlerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * REST support tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridCacheCommandHandlerSelfTest.class,
    GridCacheAtomicCommandHandlerSelfTest.class,
    GridCacheMetadataCommandTest.class,
    GridLogCommandHandlerTest.class,
    GridQueryCommandHandlerTest.class,
    CacheTopologyCommandHandlerTest.class,
    RestProtocolStartTest.class,
    RestProcessorInitializationTest.class,
    RestProcessorHangTest.class
})
public class IgniteRestHandlerTestSuite {
}
