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

package org.apache.ignite.internal.processors.cache.persistence.cdc;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_FSYNC_WITH_DEDICATED_WORKER;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_MMAP;

/** */
@WithSystemProperty(key = IGNITE_WAL_FSYNC_WITH_DEDICATED_WORKER, value = "true")
@WithSystemProperty(key = IGNITE_WAL_MMAP, value = "false")
public class RealtimeCdcBufferDedicatedWorkerTest extends RealtimeCdcBufferTest {
    /** Override params to test only FSYNC mode. */
    @Parameterized.Parameters(name = "walMode={0}")
    public static List<Object[]> params() {
        List<Object[]> param = new ArrayList<>();

        param.add(new Object[] {WALMode.FSYNC});

        return param;
    }
}
