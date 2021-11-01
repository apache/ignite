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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.sample.DiscoveryConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
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
    ) throws Exception {
        assertEquals(5000, fieldCfg.joinTimeout().value());

        assertEquals(100, paramCfg.joinTimeout().value());

        paramCfg.change(d -> d.changeJoinTimeout(200)).get(1, TimeUnit.SECONDS);

        assertEquals(200, paramCfg.joinTimeout().value());

        paramCfg.joinTimeout().update(300).get(1, TimeUnit.SECONDS);

        assertEquals(300, paramCfg.joinTimeout().value());
    }

    /** Tests that notifications work on injected configuration instance. */
    @Test
    public void notifications() throws Exception {
        List<String> log = new ArrayList<>();

        fieldCfg.listen(ctx -> {
            log.add("update");

            return completedFuture(null);
        });

        fieldCfg.joinTimeout().listen(ctx -> {
            log.add("join");

            return completedFuture(null);
        });

        fieldCfg.failureDetectionTimeout().listen(ctx -> {
            log.add("failure");

            return completedFuture(null);
        });

        fieldCfg.change(change -> change.changeJoinTimeout(1000_000)).get(1, TimeUnit.SECONDS);

        assertEquals(List.of("update", "join"), log);

        log.clear();

        fieldCfg.failureDetectionTimeout().update(2000_000).get(1, TimeUnit.SECONDS);

        assertEquals(List.of("update", "failure"), log);
    }

    /** Tests that internal configuration extensions work properly on injected configuration instance. */
    @Test
    public void internalConfiguration(
        @InjectConfiguration(internalExtensions = {ExtendedConfigurationSchema.class}) BasicConfiguration cfg
    ) throws Exception {
        assertThat(cfg, is(instanceOf(ExtendedConfiguration.class)));

        assertEquals(1, cfg.visible().value());

        assertEquals(2, ((ExtendedConfiguration)cfg).invisible().value());

        cfg.change(change -> {
            assertThat(change, is(instanceOf(ExtendedChange.class)));

            change.changeVisible(3);

            ((ExtendedChange)change).changeInvisible(4);
        }).get(1, TimeUnit.SECONDS);

        assertEquals(3, cfg.visible().value());

        assertEquals(4, ((ExtendedConfiguration)cfg).invisible().value());
    }
}
