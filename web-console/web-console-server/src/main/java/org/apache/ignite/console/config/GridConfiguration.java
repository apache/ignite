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

package org.apache.ignite.console.config;

import java.util.Collections;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

/**
 * Grid instance configuration.
 */
@Configuration
@ImportResource("classpath:ignite-config.xml")
public class GridConfiguration {
    /**
     * @param cfg Grid configuration.
     */
    @Bean(destroyMethod = "close")
    public IgniteEx igniteInstance(@Autowired IgniteConfiguration cfg) {
        IgniteEx ignite = (IgniteEx)Ignition.start(cfg);

        ignite.cluster().active(true);

        return ignite;
    }
}
