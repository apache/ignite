package org.apache.ignite.spring.sessions;

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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.session.FlushMode;
import org.springframework.session.IndexResolver;
import org.springframework.session.SaveMode;
import org.springframework.session.Session;
import org.springframework.session.config.SessionRepositoryCustomizer;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link IgniteHttpSessionConfiguration}.
 */
public class IgniteHttpSessionConfigurationTest {
    /** */
    private static final String MAP_NAME = "spring:test:sessions";

    /** */
    private static final int MAX_INACTIVE_INTERVAL_IN_SECONDS = 600;

    /** */
    private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    /** */
    @AfterEach
    void closeContext() {
        if (this.context != null)
            this.context.close();
    }

    /** */
    @Test
    void noIgniteConfiguration() {
        assertThatExceptionOfType(BeanCreationException.class)
                .isThrownBy(() -> registerAndRefresh(NoIgniteConfiguration.class)).withMessageContaining("Ignite");
    }

    /** */
    @Test
    void defaultConfiguration() {
        registerAndRefresh(DefaultConfiguration.class);

        assertThat(this.context.getBean(IgniteIndexedSessionRepository.class)).isNotNull();
    }

    /** */
    @Test
    void customTableName() {
        registerAndRefresh(CustomSessionMapNameConfiguration.class);

        IgniteIndexedSessionRepository repository = this.context.getBean(IgniteIndexedSessionRepository.class);
        IgniteHttpSessionConfiguration configuration = this.context.getBean(IgniteHttpSessionConfiguration.class);
        assertThat(repository).isNotNull();
        assertThat(ReflectionTestUtils.getField(configuration, "sessionMapName")).isEqualTo(MAP_NAME);
    }

    /** */
    @Test
    void setCustomSessionMapName() {
        registerAndRefresh(BaseConfiguration.class, CustomSessionMapNameSetConfiguration.class);

        IgniteIndexedSessionRepository repository = this.context.getBean(IgniteIndexedSessionRepository.class);
        IgniteHttpSessionConfiguration configuration = this.context.getBean(IgniteHttpSessionConfiguration.class);
        assertThat(repository).isNotNull();
        assertThat(ReflectionTestUtils.getField(configuration, "sessionMapName")).isEqualTo(MAP_NAME);
    }

    /** */
    @Test
    void setCustomMaxInactiveIntervalInSeconds() {
        registerAndRefresh(BaseConfiguration.class, CustomMaxInactiveIntervalInSecondsSetConfiguration.class);

        IgniteIndexedSessionRepository repository = this.context.getBean(IgniteIndexedSessionRepository.class);
        assertThat(repository).isNotNull();
        assertThat(ReflectionTestUtils.getField(repository, "defaultMaxInactiveInterval"))
                .isEqualTo(MAX_INACTIVE_INTERVAL_IN_SECONDS);
    }

    /** */
    @Test
    void customMaxInactiveIntervalInSeconds() {
        registerAndRefresh(CustomMaxInactiveIntervalInSecondsConfiguration.class);

        IgniteIndexedSessionRepository repository = this.context.getBean(IgniteIndexedSessionRepository.class);
        assertThat(repository).isNotNull();
        assertThat(ReflectionTestUtils.getField(repository, "defaultMaxInactiveInterval"))
                .isEqualTo(MAX_INACTIVE_INTERVAL_IN_SECONDS);
    }

    /** */
    @Test
    void customFlushImmediately() {
        registerAndRefresh(CustomFlushImmediatelyConfiguration.class);

        IgniteIndexedSessionRepository repository = this.context.getBean(IgniteIndexedSessionRepository.class);
        assertThat(repository).isNotNull();
        assertThat(ReflectionTestUtils.getField(repository, "flushMode")).isEqualTo(FlushMode.IMMEDIATE);
    }

    /** */
    @Test
    void setCustomFlushImmediately() {
        registerAndRefresh(BaseConfiguration.class, CustomFlushImmediatelySetConfiguration.class);

        IgniteIndexedSessionRepository repository = this.context.getBean(IgniteIndexedSessionRepository.class);
        assertThat(repository).isNotNull();
        assertThat(ReflectionTestUtils.getField(repository, "flushMode")).isEqualTo(FlushMode.IMMEDIATE);
    }

    /** */
    @Test
    void customSaveModeAnnotation() {
        registerAndRefresh(BaseConfiguration.class, CustomSaveModeExpressionAnnotationConfiguration.class);
        assertThat(this.context.getBean(IgniteIndexedSessionRepository.class)).hasFieldOrPropertyWithValue("saveMode",
                SaveMode.ALWAYS);
    }

    /** */
    @Test
    void customSaveModeSetter() {
        registerAndRefresh(BaseConfiguration.class, CustomSaveModeExpressionSetterConfiguration.class);
        assertThat(this.context.getBean(IgniteIndexedSessionRepository.class)).hasFieldOrPropertyWithValue("saveMode",
                SaveMode.ALWAYS);
    }

    /** */
    @Test
    void qualifiedIgniteConfiguration() {
        registerAndRefresh(QualifiedIgniteConfiguration.class);

        IgniteIndexedSessionRepository repository = this.context.getBean(IgniteIndexedSessionRepository.class);
        Ignite ignite = this.context.getBean("qualifiedIgnite", Ignite.class);
        assertThat(repository).isNotNull();
        assertThat(ignite).isNotNull();
        assertThat(ReflectionTestUtils.getField(repository, "sessions"))
                .isEqualTo(QualifiedIgniteConfiguration.qualifiedIgniteSessions);
    }

    /** */
    @Test
    void primaryIgniteConfiguration() {
        registerAndRefresh(PrimaryIgniteConfiguration.class);

        IgniteIndexedSessionRepository repository = this.context.getBean(IgniteIndexedSessionRepository.class);
        Ignite ignite = this.context.getBean("primaryIgnite", Ignite.class);
        assertThat(repository).isNotNull();
        assertThat(ignite).isNotNull();
        assertThat(ReflectionTestUtils.getField(repository, "sessions"))
                .isEqualTo(PrimaryIgniteConfiguration.primaryIgniteSessions);
    }

    /** */
    @Test
    void qualifiedAndPrimaryIgniteConfiguration() {
        registerAndRefresh(QualifiedAndPrimaryIgniteConfiguration.class);

        IgniteIndexedSessionRepository repository = this.context.getBean(IgniteIndexedSessionRepository.class);
        Ignite ignite = this.context.getBean("qualifiedIgnite", Ignite.class);
        assertThat(repository).isNotNull();
        assertThat(ignite).isNotNull();
        assertThat(ReflectionTestUtils.getField(repository, "sessions"))
                .isEqualTo(QualifiedAndPrimaryIgniteConfiguration.qualifiedIgniteSessions);
    }

    /** */
    @Test
    void namedIgniteConfiguration() {
        registerAndRefresh(NamedIgniteConfiguration.class);

        IgniteIndexedSessionRepository repository = this.context.getBean(IgniteIndexedSessionRepository.class);
        Ignite ignite = this.context.getBean("ignite", Ignite.class);
        assertThat(repository).isNotNull();
        assertThat(ignite).isNotNull();
        assertThat(ReflectionTestUtils.getField(repository, "sessions"))
                .isEqualTo(NamedIgniteConfiguration.igniteSessions);
    }

    /** */
    @Test
    void multipleIgniteConfiguration() {
        assertThatExceptionOfType(BeanCreationException.class)
                .isThrownBy(() -> registerAndRefresh(MultipleIgniteConfiguration.class))
                .withMessageContaining("expected single matching bean but found 2");
    }

    /** */
    @Test
    void customIndexResolverConfiguration() {
        registerAndRefresh(CustomIndexResolverConfiguration.class);
        IgniteIndexedSessionRepository repository = this.context.getBean(IgniteIndexedSessionRepository.class);
        @SuppressWarnings("unchecked")
        IndexResolver<Session> indexResolver = this.context.getBean(IndexResolver.class);
        assertThat(repository).isNotNull();
        assertThat(indexResolver).isNotNull();
        assertThat(repository).hasFieldOrPropertyWithValue("indexResolver", indexResolver);
    }

    /** */
    @Test
    void sessionRepositoryCustomizer() {
        registerAndRefresh(SessionRepositoryCustomizerConfiguration.class);
        IgniteIndexedSessionRepository sessionRepository = this.context.getBean(IgniteIndexedSessionRepository.class);
        assertThat(sessionRepository).hasFieldOrPropertyWithValue("defaultMaxInactiveInterval",
                MAX_INACTIVE_INTERVAL_IN_SECONDS);
    }

    /** */
    private void registerAndRefresh(Class<?>... annotatedClasses) {
        this.context.register(annotatedClasses);
        this.context.refresh();
    }

    /** */
    @Configuration
    @EnableIgniteHttpSession
    static class NoIgniteConfiguration {

    }

    /** */
    static class BaseConfiguration {

        /** */
        @SuppressWarnings("unchecked")
        static IgniteCache<Object, Object> defaultIgniteSessions = mock(IgniteCache.class);

        /** */
        @Bean
        Ignite defaultIgnite() {
            Ignite ignite = mock(Ignite.class);
            given(ignite.getOrCreateCache(ArgumentMatchers.<CacheConfiguration<Object, Object>>any()))
                    .willReturn(defaultIgniteSessions);
            return ignite;
        }

    }

    /** */
    @Configuration
    @EnableIgniteHttpSession
    static class DefaultConfiguration extends BaseConfiguration {

    }

    /** */
    @Configuration
    @EnableIgniteHttpSession(sessionMapName = MAP_NAME)
    static class CustomSessionMapNameConfiguration extends BaseConfiguration {

    }

    /** */
    @Configuration
    static class CustomSessionMapNameSetConfiguration extends IgniteHttpSessionConfiguration {
        /** */
        CustomSessionMapNameSetConfiguration() {
            setSessionMapName(MAP_NAME);
        }

    }

    /** */
    @Configuration
    static class CustomMaxInactiveIntervalInSecondsSetConfiguration extends IgniteHttpSessionConfiguration {
        /** */
        CustomMaxInactiveIntervalInSecondsSetConfiguration() {
            setMaxInactiveIntervalInSeconds(MAX_INACTIVE_INTERVAL_IN_SECONDS);
        }

    }

    /** */
    @Configuration
    @EnableIgniteHttpSession(maxInactiveIntervalInSeconds = MAX_INACTIVE_INTERVAL_IN_SECONDS)
    static class CustomMaxInactiveIntervalInSecondsConfiguration extends BaseConfiguration {

    }

    /** */
    @Configuration
    static class CustomFlushImmediatelySetConfiguration extends IgniteHttpSessionConfiguration {
        /** */
        CustomFlushImmediatelySetConfiguration() {
            setFlushMode(FlushMode.IMMEDIATE);
        }

    }

    /** */
    @EnableIgniteHttpSession(saveMode = SaveMode.ALWAYS)
    static class CustomSaveModeExpressionAnnotationConfiguration {

    }

    /** */
    @Configuration
    static class CustomSaveModeExpressionSetterConfiguration extends IgniteHttpSessionConfiguration {
        /** */
        CustomSaveModeExpressionSetterConfiguration() {
            setSaveMode(SaveMode.ALWAYS);
        }

    }

    /** */
    @Configuration
    @EnableIgniteHttpSession(flushMode = FlushMode.IMMEDIATE)
    static class CustomFlushImmediatelyConfiguration extends BaseConfiguration {

    }

    /** */
    @Configuration
    @EnableIgniteHttpSession
    static class QualifiedIgniteConfiguration extends BaseConfiguration {

        /** */
        @SuppressWarnings("unchecked")
        static IgniteCache<Object, Object> qualifiedIgniteSessions = mock(IgniteCache.class);

        /** */
        @Bean
        @SpringSessionIgnite
        Ignite qualifiedIgnite() {
            Ignite ignite = mock(Ignite.class);
            given(ignite.getOrCreateCache(ArgumentMatchers.<CacheConfiguration<Object, Object>>any()))
                    .willReturn(qualifiedIgniteSessions);
            return ignite;
        }

    }

    /** */
    @Configuration
    @EnableIgniteHttpSession
    static class PrimaryIgniteConfiguration extends BaseConfiguration {

        /** */
        @SuppressWarnings("unchecked")
        static IgniteCache<Object, Object> primaryIgniteSessions = mock(IgniteCache.class);

        /** */
        @Bean
        @Primary
        Ignite primaryIgnite() {
            Ignite ignite = mock(Ignite.class);
            given(ignite.getOrCreateCache(ArgumentMatchers.<CacheConfiguration<Object, Object>>any()))
                    .willReturn(primaryIgniteSessions);
            return ignite;
        }

    }

    /** */
    @Configuration
    @EnableIgniteHttpSession
    static class QualifiedAndPrimaryIgniteConfiguration extends BaseConfiguration {

        /** */
        @SuppressWarnings("unchecked")
        static IgniteCache<Object, Object> qualifiedIgniteSessions = mock(IgniteCache.class);

        /** */
        @SuppressWarnings("unchecked")
        static IgniteCache<Object, Object> primaryIgniteSessions = mock(IgniteCache.class);

        /** */
        @Bean
        @SpringSessionIgnite
        Ignite qualifiedIgnite() {
            Ignite ignite = mock(Ignite.class);
            given(ignite.getOrCreateCache(ArgumentMatchers.<CacheConfiguration<Object, Object>>any()))
                    .willReturn(qualifiedIgniteSessions);
            return ignite;
        }

        /** */
        @Bean
        @Primary
        Ignite primaryIgnite() {
            Ignite ignite = mock(Ignite.class);
            given(ignite.getOrCreateCache(ArgumentMatchers.<CacheConfiguration<Object, Object>>any()))
                    .willReturn(primaryIgniteSessions);
            return ignite;
        }

    }

    /** */
    @Configuration
    @EnableIgniteHttpSession
    static class NamedIgniteConfiguration extends BaseConfiguration {

        /** */
        @SuppressWarnings("unchecked")
        static IgniteCache<Object, Object> igniteSessions = mock(IgniteCache.class);

        /** */
        @Bean
        Ignite ignite() {
            Ignite ignite = mock(Ignite.class);
            given(ignite.getOrCreateCache(ArgumentMatchers.<CacheConfiguration<Object, Object>>any()))
                    .willReturn(igniteSessions);
            return ignite;
        }

    }

    /** */
    @Configuration
    @EnableIgniteHttpSession
    static class MultipleIgniteConfiguration extends BaseConfiguration {

        /** */
        @SuppressWarnings("unchecked")
        static IgniteCache<Object, Object> secondaryIgniteSessions = mock(IgniteCache.class);

        /** */
        @Bean
        Ignite secondaryIgnite() {
            Ignite ignite = mock(Ignite.class);
            given(ignite.getOrCreateCache(ArgumentMatchers.<CacheConfiguration<Object, Object>>any()))
                    .willReturn(secondaryIgniteSessions);
            return ignite;
        }

    }

    /** */
    @EnableIgniteHttpSession
    static class CustomIndexResolverConfiguration extends BaseConfiguration {

        /** */
        @Bean
        @SuppressWarnings("unchecked")
        IndexResolver<Session> indexResolver() {
            return mock(IndexResolver.class);
        }

    }

    /** */
    @EnableIgniteHttpSession
    static class SessionRepositoryCustomizerConfiguration extends BaseConfiguration {

        /** */
        @Bean
        @Order(0)
        SessionRepositoryCustomizer<IgniteIndexedSessionRepository> sessionRepositoryCustomizerOne() {
            return (sessionRepository) -> sessionRepository.setDefaultMaxInactiveInterval(0);
        }

        /** */
        @Bean
        @Order(1)
        SessionRepositoryCustomizer<IgniteIndexedSessionRepository> sessionRepositoryCustomizerTwo() {
            return (sessionRepository) -> sessionRepository
                    .setDefaultMaxInactiveInterval(MAX_INACTIVE_INTERVAL_IN_SECONDS);
        }
    }
}
