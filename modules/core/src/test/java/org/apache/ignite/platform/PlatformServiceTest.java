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

package org.apache.ignite.platform;

import java.sql.Timestamp;
import org.apache.ignite.Ignite;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Calendar.JANUARY;

/** */
public class PlatformServiceTest extends GridCommonAbstractTest {
    @Test
    public void testService() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.compute().execute("org.apache.ignite.platform.PlatformDeployServiceTask", "javaService");

        PlatformDeployServiceTask.PlatformTestService svc = ignite.services().service("javaService");

        Timestamp input = new Timestamp(1992, JANUARY, 1, 0, 0, 0, 0);

        svc.testDateTime(input);

        Thread.sleep(50_000L);
    }
}
