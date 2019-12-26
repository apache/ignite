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

package org.apache.ignite.springsession.annotation.config;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.springsession.IgniteSessionRepository;
import org.apache.ignite.springsession.annotation.EnableIgniteHttpSession;
import org.apache.ignite.springsession.annotation.SpringSessionIgniteInstance;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.session.MapSession;
import org.springframework.session.config.annotation.web.http.SpringHttpSessionConfiguration;
import org.springframework.util.StringUtils;

/**
 * Ignite http session configuration.
 */
@Configuration
public class IgniteHttpSessionConfiguration extends SpringHttpSessionConfiguration implements ImportAware {
	/** Session cache name. */
	private String sessionCacheName = IgniteSessionRepository.DFLT_SESSION_STORAGE_NAME;

	/** Max inactive time interval for session to expire. */
	private Integer maxInactiveIntervalInSeconds = MapSession.DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS;

	/** Ignite instance to use. */
	private Ignite ignite;

	/** Gets session repository.
	 *
	 * @return Session repository backed by Ignite.
	 */
	@Bean
	public IgniteSessionRepository sessionRepository() {
		IgniteSessionRepository sessionRepository = new IgniteSessionRepository(ignite);

		sessionRepository.setSessionCacheName(this.sessionCacheName);

		sessionRepository.setDefaultMaxInactiveInterval(this.maxInactiveIntervalInSeconds);

		return sessionRepository;
	}

	/**
	 * Sets max inactive time interval for session to expire.
	 *
	 * @param maxInactiveIntervalInSeconds Max inactive time interval for session to expire.
	 */
	public void setMaxInactiveIntervalInSeconds(Integer maxInactiveIntervalInSeconds) {
		this.maxInactiveIntervalInSeconds = maxInactiveIntervalInSeconds;
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
	 * Sets Ignite instance to be used by repository.
	 *
	 * @param springSessionIgniteInstance Ignite instance.
	 * @param igniteInstance Ignite instance.
	 */
	@Autowired
	public void setIgnite(
		@SpringSessionIgniteInstance ObjectFactory<Ignite> springSessionIgniteInstance,
		ObjectFactory<Ignite> igniteInstance ) {
		Ignite igniteToUse = springSessionIgniteInstance.getObject();

		if (igniteToUse == null)
			igniteInstance.getObject();

		this.ignite = igniteToUse;
	}


	/** {@inheritDoc} */
	@Override
	public void setImportMetadata(AnnotationMetadata importMetadata) {
		Map<String, Object> attributeMap = importMetadata
				.getAnnotationAttributes(EnableIgniteHttpSession.class.getName());

		AnnotationAttributes attributes = AnnotationAttributes.fromMap(attributeMap);

		this.maxInactiveIntervalInSeconds =
				attributes.getNumber("maxInactiveIntervalInSeconds");

		String sessionCacheNameValue = attributes.getString("sessionCacheName");

		if (StringUtils.hasText(sessionCacheNameValue))
			this.sessionCacheName = sessionCacheNameValue;
	}
}
