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

package org.apache.ignite.spring.sessions;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.web.WebAppConfiguration;

import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;
import static org.apache.ignite.spring.sessions.IgniteIndexedSessionRepository.DEFAULT_SESSION_MAP_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link IgniteIndexedSessionRepository} using embedded Ignite instance.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
@WebAppConfiguration
public class IgniteClientIndexedSessionRepositoryTest extends AbstractIgniteIndexedSessionRepositoryTest {
    /** */
    @Autowired
    private IgniteClient cli;

    /** */
    @BeforeAll
    static void setUpClass() {
        Ignition.stopAll(true);

        IgniteTestUtils.embeddedIgniteServer();
    }

    /** */
    @AfterAll
    static void tearDownClass() {
        Ignition.stopAll(true);
    }

    /** */
    @BeforeEach
    void setUp() {
        cli.cache(DEFAULT_SESSION_MAP_NAME).clear();
    }

    /** */
    @EnableIgniteHttpSession
    @Configuration
    static class IgniteSessionConfig {
        /** */
        @Bean
        IgniteClient ignite() {
            return Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:" + DFLT_PORT));
        }
    }

    /** */
    @Test
    void createAndDestroySession() {
        IgniteSession sesToSave = repo.createSession();
        String sesId = sesToSave.getId();

        ClientCache<String, IgniteSession> cache = cli.cache(DEFAULT_SESSION_MAP_NAME);

        assertThat(cache.size()).isEqualTo(0);

        repo.save(sesToSave);

        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.get(sesId)).isEqualTo(sesToSave);

        repo.deleteById(sesId);

        assertThat(cache.size()).isEqualTo(0);
    }
}
