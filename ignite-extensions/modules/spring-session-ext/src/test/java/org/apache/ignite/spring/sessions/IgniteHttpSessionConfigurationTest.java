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
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.springframework.test.util.ReflectionTestUtils.getField;

/**
 * Tests for {@link IgniteHttpSessionConfiguration}.
 */
public class IgniteHttpSessionConfigurationTest {
    /** */
    private static final String MAP_NAME = "spring:test:sessions";

    /** */
    private static final int MAX_INACTIVE_INTERVAL_IN_SECONDS = 600;

    /** */
    private final AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();

    /** */
    @AfterEach
    void closeContext() {
        this.ctx.close();
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

        assertThat(this.ctx.getBean(IgniteIndexedSessionRepository.class)).isNotNull();
    }

    /** */
    @Test
    void customTableName() {
        registerAndRefresh(CustomSessionMapNameConfiguration.class);

        IgniteIndexedSessionRepository repo = this.ctx.getBean(IgniteIndexedSessionRepository.class);
        IgniteHttpSessionConfiguration configuration = this.ctx.getBean(IgniteHttpSessionConfiguration.class);
        assertThat(repo).isNotNull();
        assertThat(getField(configuration, "sesMapName")).isEqualTo(MAP_NAME);
    }

    /** */
    @Test
    void setCustomSessionMapName() {
        registerAndRefresh(BaseConfiguration.class, CustomSessionMapNameSetConfiguration.class);

        IgniteIndexedSessionRepository repo = this.ctx.getBean(IgniteIndexedSessionRepository.class);
        IgniteHttpSessionConfiguration configuration = this.ctx.getBean(IgniteHttpSessionConfiguration.class);
        assertThat(repo).isNotNull();
        assertThat(getField(configuration, "sesMapName")).isEqualTo(MAP_NAME);
    }

    /** */
    @Test
    void setCustomMaxInactiveIntervalInSeconds() {
        registerAndRefresh(BaseConfiguration.class, CustomMaxInactiveIntervalInSecondsSetConfiguration.class);

        IgniteIndexedSessionRepository repo = this.ctx.getBean(IgniteIndexedSessionRepository.class);
        assertThat(repo).isNotNull();
        assertThat(getField(repo, "dfltMaxInactiveInterval"))
                .isEqualTo(MAX_INACTIVE_INTERVAL_IN_SECONDS);
    }

    /** */
    @Test
    void customMaxInactiveIntervalInSeconds() {
        registerAndRefresh(CustomMaxInactiveIntervalInSecondsConfiguration.class);

        IgniteIndexedSessionRepository repo = this.ctx.getBean(IgniteIndexedSessionRepository.class);
        assertThat(repo).isNotNull();
        assertThat(getField(repo, "dfltMaxInactiveInterval"))
                .isEqualTo(MAX_INACTIVE_INTERVAL_IN_SECONDS);
    }

    /** */
    @Test
    void customFlushImmediately() {
        registerAndRefresh(CustomFlushImmediatelyConfiguration.class);

        IgniteIndexedSessionRepository repo = this.ctx.getBean(IgniteIndexedSessionRepository.class);
        assertThat(repo).isNotNull();
        assertThat(getField(repo, "flushMode")).isEqualTo(FlushMode.IMMEDIATE);
    }

    /** */
    @Test
    void setCustomFlushImmediately() {
        registerAndRefresh(BaseConfiguration.class, CustomFlushImmediatelySetConfiguration.class);

        IgniteIndexedSessionRepository repo = this.ctx.getBean(IgniteIndexedSessionRepository.class);
        assertThat(repo).isNotNull();
        assertThat(getField(repo, "flushMode")).isEqualTo(FlushMode.IMMEDIATE);
    }

    /** */
    @Test
    void customSaveModeAnnotation() {
        registerAndRefresh(BaseConfiguration.class, CustomSaveModeExpressionAnnotationConfiguration.class);
        assertThat(this.ctx.getBean(IgniteIndexedSessionRepository.class)).hasFieldOrPropertyWithValue("saveMode",
                SaveMode.ALWAYS);
    }

    /** */
    @Test
    void customSaveModeSetter() {
        registerAndRefresh(BaseConfiguration.class, CustomSaveModeExpressionSetterConfiguration.class);
        assertThat(this.ctx.getBean(IgniteIndexedSessionRepository.class)).hasFieldOrPropertyWithValue("saveMode",
                SaveMode.ALWAYS);
    }

    /** */
    @Test
    void qualifiedIgniteConfiguration() {
        registerAndRefresh(QualifiedIgniteConfiguration.class);

        IgniteIndexedSessionRepository repo = this.ctx.getBean(IgniteIndexedSessionRepository.class);
        Ignite ignite = this.ctx.getBean("qualifiedIgnite", Ignite.class);
        assertThat(repo).isNotNull();
        assertThat(ignite).isNotNull();
        assertThat(getField(getField(repo, "sessions"), "cache"))
            .isEqualTo(QualifiedIgniteConfiguration.qualifiedIgniteSessions);
    }

    /** */
    @Test
    void primaryIgniteConfiguration() {
        registerAndRefresh(PrimaryIgniteConfiguration.class);

        IgniteIndexedSessionRepository repo = this.ctx.getBean(IgniteIndexedSessionRepository.class);
        Ignite ignite = this.ctx.getBean("primaryIgnite", Ignite.class);
        assertThat(repo).isNotNull();
        assertThat(ignite).isNotNull();
        assertThat(getField(getField(repo, "sessions"), "cache"))
            .isEqualTo(PrimaryIgniteConfiguration.primaryIgniteSessions);
    }

    /** */
    @Test
    void qualifiedAndPrimaryIgniteConfiguration() {
        registerAndRefresh(QualifiedAndPrimaryIgniteConfiguration.class);

        IgniteIndexedSessionRepository repo = this.ctx.getBean(IgniteIndexedSessionRepository.class);
        Ignite ignite = this.ctx.getBean("qualifiedIgnite", Ignite.class);
        assertThat(repo).isNotNull();
        assertThat(ignite).isNotNull();
        assertThat(getField(getField(repo, "sessions"), "cache"))
            .isEqualTo(QualifiedAndPrimaryIgniteConfiguration.qualifiedIgniteSessions);
    }

    /** */
    @Test
    void namedIgniteConfiguration() {
        registerAndRefresh(NamedIgniteConfiguration.class);

        IgniteIndexedSessionRepository repo = this.ctx.getBean(IgniteIndexedSessionRepository.class);
        Ignite ignite = this.ctx.getBean("ignite", Ignite.class);
        assertThat(repo).isNotNull();
        assertThat(ignite).isNotNull();
        assertThat(getField(getField(repo, "sessions"), "cache"))
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
        IgniteIndexedSessionRepository repo = this.ctx.getBean(IgniteIndexedSessionRepository.class);
        @SuppressWarnings("unchecked")
        IndexResolver<Session> idxResolver = this.ctx.getBean(IndexResolver.class);
        assertThat(repo).isNotNull();
        assertThat(idxResolver).isNotNull();
        assertThat(repo).hasFieldOrPropertyWithValue("idxResolver", idxResolver);
    }

    /** */
    @Test
    void sessionRepositoryCustomizer() {
        registerAndRefresh(SessionRepositoryCustomizerConfiguration.class);
        IgniteIndexedSessionRepository sesRepo = this.ctx.getBean(IgniteIndexedSessionRepository.class);
        assertThat(sesRepo).hasFieldOrPropertyWithValue("dfltMaxInactiveInterval",
                MAX_INACTIVE_INTERVAL_IN_SECONDS);
    }

    /** */
    private void registerAndRefresh(Class<?>... annotatedClasses) {
        this.ctx.register(annotatedClasses);
        this.ctx.refresh();
    }

    /** */
    @Configuration
    @EnableIgniteHttpSession
    static class NoIgniteConfiguration {
        // No-op.
    }

    /** */
    static class BaseConfiguration {
        /** */
        @SuppressWarnings("unchecked")
        static IgniteCache<Object, Object> dfltIgniteSessions = mock(IgniteCache.class);

        /** */
        @Bean
        Ignite defaultIgnite() {
            return mockedIgnite(dfltIgniteSessions);
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
            return mockedIgnite(qualifiedIgniteSessions);
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
            return mockedIgnite(primaryIgniteSessions);
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
            return mockedIgnite(qualifiedIgniteSessions);
        }

        /** */
        @Bean
        @Primary
        Ignite primaryIgnite() {
            return mockedIgnite(primaryIgniteSessions);
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
            return mockedIgnite(igniteSessions);
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
            return mockedIgnite(secondaryIgniteSessions);
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

    /**
     * @param cache Cache instance.
     */
    private static Ignite mockedIgnite(IgniteCache<Object, Object> cache) {
        IgniteEx ignite = mock(IgniteEx.class);
        GridKernalContext ctx = mock(GridKernalContext.class);
        GridQueryProcessor qryProc = mock(GridQueryProcessor.class);
        
        given(qryProc.querySqlFields(any(SqlFieldsQuery.class), anyBoolean())).willReturn(mock(FieldsQueryCursor.class));
        given(ctx.query()).willReturn(qryProc);
        given(ignite.context()).willReturn(ctx);
        given(ignite.cache(any())).willReturn(cache);
        return ignite;
    }
}
