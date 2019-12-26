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

package org.apache.ignite.springsession;

import javax.annotation.PostConstruct;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.springframework.session.SessionRepository;
import org.springframework.stereotype.Component;

/**
 * Ignite session repository implementation.
 */
@Component
public class IgniteSessionRepository implements SessionRepository<IgniteSession> {
	/** Default session cache name. */
	public static final String DFLT_SESSION_STORAGE_NAME = "spring.session.cache";

	/** Default active timeout for session. */
	private Long defaultActiveTimeout;

	/** Session cache. */
	private IgniteCache<String, IgniteSession> sessionCache;

	/** Ignite node that is used to manage sessions. */
	@IgniteInstanceResource
	private Ignite ignite;

	/** Session cache name. */
	private String sessionCacheName;

	/** Default max inactive time interval for session to expire. */
	private Integer defaultMaxInactiveInterval;

	/**
	 * Constructor.
	 *
	 * @param ignite Ignite instance.
	 */
	public IgniteSessionRepository(Ignite ignite) {
		this.ignite = ignite;
	}

	/**
	 * Gets session cache configuration instance.
	 *
	 * @return Session cache configuration.
	 */
	private CacheConfiguration<String, IgniteSession> getSessionCacheConfig() {
		CacheConfiguration<String, IgniteSession> sesCacheCfg = new CacheConfiguration<String, IgniteSession>();

		sesCacheCfg.setName(this.sessionCacheName);
		sesCacheCfg.setCacheMode(CacheMode.REPLICATED);

		return sesCacheCfg;
	}

	/** Initializes cache for sessions. */
	@PostConstruct
	public void init() {
		this.sessionCache = this.ignite.getOrCreateCache(this.getSessionCacheConfig());
	}

	/** {@inheritDoc} */
	@Override
	public IgniteSession createSession() {
		IgniteSession session = new IgniteSession();

		return session;
	}

	/** {@inheritDoc} */
	@Override
	public void save(final IgniteSession session) {
		if (!session.getId().equals(session.getOriginalId())) {
			delete(session.getOriginalId());

			session.setOriginalId(session.getId());
		}

		this.sessionCache.put(session.getId(), session);
	}

	/** {@inheritDoc} */
	@Override
	public IgniteSession getSession(String id) {
		IgniteSession session = this.sessionCache.get(id);

		if (session == null)
			return null;

		if (session.isExpired()) {
			delete(id);
			return null;
		}

		return session;
	}

	/** {@inheritDoc} */
	@Override
	public void delete(String id) {
		this.sessionCache.remove(id);
	}

	/**
	 * Sets session cache name.
	 *
	 * @param name Session cache naming.
	 */
	public void setSessionCacheName(String name) {
		this.sessionCacheName = name;
	}

	/**
	 * Sets default max inactive time interval for session.
	 *
	 * @param interval Interval for session to be considered active.
	 */
	public void setDefaultMaxInactiveInterval(Integer interval) {
		this.defaultMaxInactiveInterval = interval;
	}
}