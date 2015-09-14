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

package org.apache.ignite.stream.mqtt;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import org.junit.After;
import org.junit.Before;

/**
 * Test for {@link MqttStreamer}.
 *
 * @author Raul Kripalani
 */
public class IgniteMqttStreamerTest extends GridCommonAbstractTest {

    /** Constructor. */
    public IgniteMqttStreamerTest() {
        super(true);
    }

    @Before @SuppressWarnings("unchecked")
    public void beforeTest() throws Exception {
        grid().<Integer, String>getOrCreateCache(defaultCacheConfiguration());

    }

    @After
    public void afterTest() throws Exception {
        grid().cache(null).clear();


    }

}