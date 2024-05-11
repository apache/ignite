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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
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

import static org.apache.ignite.spring.sessions.IgniteIndexedSessionRepository.DEFAULT_SESSION_MAP_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link IgniteIndexedSessionRepository} using embedded Ignite instance.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
@WebAppConfiguration
public class EmbeddedIgniteIndexedSessionRepositoryTest extends AbstractIgniteIndexedSessionRepositoryTest {
    /** */
    @Autowired
    private Ignite ignite;

    /** */
    @BeforeAll
    static void setUpClass() {
        Ignition.stopAll(true);
    }

    /** */
    @AfterAll
    static void tearDownClass() {
        Ignition.stopAll(true);
    }

    /** */
    @BeforeEach
    void setUp() {
        ignite.cache(DEFAULT_SESSION_MAP_NAME).clear();
    }

    /** */
    @EnableIgniteHttpSession
    @Configuration
    static class IgniteSessionConfig {
        /** */
        @Bean
        Ignite ignite() {
            return IgniteTestUtils.embeddedIgniteServer();
        }
    }

    /** */
    @Test
    void createAndDestroySession() {
        IgniteSession sesToSave = repo.createSession();
        String sesId = sesToSave.getId();

        IgniteCache<String, IgniteSession> cache = ignite.getOrCreateCache(DEFAULT_SESSION_MAP_NAME);

        assertThat(cache.size()).isEqualTo(0);

        repo.save(sesToSave);

        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.get(sesId)).isEqualTo(sesToSave);

        repo.deleteById(sesId);

        assertThat(cache.size()).isEqualTo(0);
    }
}
