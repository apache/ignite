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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.ignite.Ignite;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.session.FlushMode;
import org.springframework.session.MapSession;
import org.springframework.session.SaveMode;
import org.springframework.session.Session;
import org.springframework.session.SessionRepository;
import org.springframework.session.web.http.SessionRepositoryFilter;

/**
 * Add this annotation to an {@code @Configuration} class to expose the
 * {@link SessionRepositoryFilter} as a bean named {@code springSessionRepositoryFilter}
 * and backed by Ignite. In order to leverage the annotation, a single {@link Ignite} must
 * be provided. For example:
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableIgniteHttpSession
 * public class IgniteHttpSessionConfig {
 *
 *     &#064;Bean
 *     public Ignite embeddedIgnite() {
 *         return IgniteEx.start();
 *     }
 *
 * }
 * </pre>
 *
 * More advanced configurations can extend {@link IgniteHttpSessionConfiguration} instead.
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@Import(IgniteHttpSessionConfiguration.class)
@Configuration(proxyBeanMethods = false)
public @interface EnableIgniteHttpSession {
    /**
     * The session timeout in seconds. By default, it is set to 1800 seconds (30 minutes).
     * This should be a non-negative integer.
     * @return the seconds a session can be inactive before expiring
     */
    int maxInactiveIntervalInSeconds() default MapSession.DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS;

    /**
     * This is the name of the Map that will be used in Ignite to store the session data.
     * Default is "spring:session:sessions".
     * @return the name of the Map to store the sessions in Ignite
     */
    String sessionMapName() default "spring:session:sessions";

    /**
     * Flush mode for the Ignite sessions. The default is {@code ON_SAVE} which only
     * updates the backing Ignite when {@link SessionRepository#save(Session)} is invoked.
     * In a web environment this happens just before the HTTP response is committed.
     * <p>
     * Setting the value to {@code IMMEDIATE} will ensure that any updates to the
     * Session are immediately written to the Ignite instance.
     * @return the {@link FlushMode} to use
     */
    FlushMode flushMode() default FlushMode.ON_SAVE;

    /**
     * Save mode for the session. The default is {@link SaveMode#ON_SET_ATTRIBUTE}, which
     * only saves changes made to session.
     * @return the save mode
     */
    SaveMode saveMode() default SaveMode.ON_SET_ATTRIBUTE;

}
