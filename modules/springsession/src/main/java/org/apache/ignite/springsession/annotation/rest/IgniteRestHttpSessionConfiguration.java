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

package org.apache.ignite.springsession.annotation.rest;

import java.util.Map;
import org.apache.ignite.springsession.annotation.IgniteRestSessionRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.session.MapSession;
import org.springframework.session.config.annotation.web.http.SpringHttpSessionConfiguration;
import org.springframework.util.StringUtils;

/**
 * Ignite configuration.
 */
@Configuration
public class IgniteRestHttpSessionConfiguration extends SpringHttpSessionConfiguration implements ImportAware {
    /** Session cache name. */
    private String sessionCacheName = IgniteRestSessionRepository.DFLT_SESSION_STORAGE_NAME;

    /** Max inactive time interval for session to expire. */
    private Integer maxInactiveIntervalInSeconds = MapSession.DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS;

    /** URL. */
    private String url;

    /**
     * Gets session repository.
     *
     * @return Ignite-backed session repository.
     */
    @Bean
    public IgniteRestSessionRepository repository() {
        IgniteRestSessionRepository repository = new IgniteRestSessionRepository();
        repository.setUrl(this.url);
        repository.setSessionCacheName(this.sessionCacheName);
        repository.setDefaultMaxInactiveInterval(maxInactiveIntervalInSeconds);
        return repository;
    }

    /**
     * Sets session cache name.
     *
     * @param sessionCacheName Session cache name.
     */
    public void setSessionCacheName(String sessionCacheName) {
        this.sessionCacheName = sessionCacheName;
    }

    /**
     * Sets max inactive time interval for session to expire.
     * @param maxInactiveIntervalInSeconds Max inactive interval in seconds.
     */
    public void setMaxInactiveIntervalInSeconds(Integer maxInactiveIntervalInSeconds) {
        this.maxInactiveIntervalInSeconds = maxInactiveIntervalInSeconds;
    }

    /**
     * Sets url.
     *
     * @param url Ignite url.
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /** {@inheritDoc} */
    @Override
    public void setImportMetadata(AnnotationMetadata importMetadata) {
        Map<String, Object> attributeMap = importMetadata
                .getAnnotationAttributes(EnableRestIgniteHttpSession.class.getName());

        AnnotationAttributes attributes = AnnotationAttributes.fromMap(attributeMap);

        this.maxInactiveIntervalInSeconds =
                attributes.getNumber("maxInactiveIntervalInSeconds");

        String sessionCacheNameValue = attributes.getString("sessionCacheName");

        if (StringUtils.hasText(sessionCacheNameValue))
            this.sessionCacheName = sessionCacheNameValue;

        this.url = attributes.getString("url");
    }
}
