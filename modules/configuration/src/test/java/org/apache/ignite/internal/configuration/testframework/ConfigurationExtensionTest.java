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

package org.apache.ignite.internal.configuration.testframework;

import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.configuration.sample.DiscoveryConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test basic scenarios of {@link ConfigurationExtension}.
 */
@ExtendWith(ConfigurationExtension.class)
class ConfigurationExtensionTest {
    /** Injected field. */
    @InjectConfiguration
    private DiscoveryConfiguration fieldCfg;

    /** Test that contains injected parameter. */
    @Test
    public void injectConfiguration(
        @InjectConfiguration("mock.joinTimeout=100") DiscoveryConfiguration paramCfg
    ) throws ExecutionException, InterruptedException {
        assertEquals(5000, fieldCfg.joinTimeout().value());

        assertEquals(100, paramCfg.joinTimeout().value());

        paramCfg.change(d -> d.changeJoinTimeout(200));

        assertEquals(200, paramCfg.joinTimeout().value());

        paramCfg.joinTimeout().update(300);

        assertEquals(300, paramCfg.joinTimeout().value());
    }
}
