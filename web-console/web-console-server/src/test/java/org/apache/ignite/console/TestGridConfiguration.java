/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * Grid config.
 */
@TestConfiguration
public class TestGridConfiguration {
    /**
     * @return Service for encoding user passwords.
     */
    @Bean
    @Primary
    public PasswordEncoder passwordEncoder() {
        return NoOpPasswordEncoder.getInstance();
    }

    /**
     * We overriding ignite creation bean for cases where the application context
     * needs to be recreated with the already running ignite instance.
     *
     * @param cfg Grid configuration.
     */
    @Primary
    @Bean(destroyMethod = "close")
    public IgniteEx igniteInstance(@Autowired IgniteConfiguration cfg) throws IgniteCheckedException {
        IgniteEx ignite = (IgniteEx) IgnitionEx.start(cfg, false);

        ignite.cluster().active(true);

        return ignite;
    }
}
