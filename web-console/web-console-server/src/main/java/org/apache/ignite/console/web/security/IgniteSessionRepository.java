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

package org.apache.ignite.console.web.security;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.springframework.context.support.MessageSourceAccessor;

import org.springframework.session.MapSession;
import org.springframework.session.Session;
import org.springframework.session.SessionRepository;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import static org.apache.ignite.console.errors.Errors.convertToDatabaseNotAvailableException;

import java.time.Duration;

/**
 * A {@link SessionRepository} backed by a Apache Ignite and that uses a {@link MapSession}.
 */
public class IgniteSessionRepository implements SessionRepository<MapSession> {
    /** */
    private final Ignite ignite;

    /** Messages accessor. */
    private final MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /** If non-null, this value is used to override {@link MapSession#setMaxInactiveIntervalInSeconds(int)}. */
    private Integer dfltMaxInactiveInterval;

    /** Session cache configuration. */
    private final CacheConfiguration<String, MapSession> cfg;

    /**
     * @param ignite Ignite.
     */
    public IgniteSessionRepository(Ignite ignite) {
       this.ignite = ignite;

        cfg = new CacheConfiguration<String, MapSession>()
            .setName("wc_sessions")
            .setCacheMode(CacheMode.REPLICATED);
    }

    /**
     * If non-null, this value is used to override {@link MapSession#setMaxInactiveIntervalInSeconds(int)}.
     *
     * @param dfltMaxInactiveInterval Number of seconds that the {@link Session} should be kept alive between client
     * requests.
     */
    public IgniteSessionRepository setDefaultMaxInactiveInterval(int dfltMaxInactiveInterval) {
        this.dfltMaxInactiveInterval = dfltMaxInactiveInterval;

        return this;
    }

    /** {@inheritDoc} */
    @Override public MapSession createSession() {
    	MapSession ses = new MapSession();

        if (dfltMaxInactiveInterval != null)
            ses.setMaxInactiveInterval(Duration.ofSeconds(dfltMaxInactiveInterval));

        return ses;
    }

    /**
     * @return Cache with sessions.
     */
    private IgniteCache<String, MapSession> cache() {
            return ignite.getOrCreateCache(cfg);
    }

    /** {@inheritDoc} */
    @Override public void save(MapSession ses) {
        try {
            cache().put(ses.getId(), new MapSession(ses));
        }
        catch (RuntimeException e) {
            throw convertToDatabaseNotAvailableException(e, messages.getMessage("err.db-not-available"));
        }
    }

    /** {@inheritDoc} */
    @Override public MapSession findById(String id) {
        try {
            MapSession ses = cache().get(id);

            if (ses == null)
                return null;

            if (ses.isExpired()) {
            	deleteById(ses.getId());

                return null;
            }

            return ses;
        }
        catch (RuntimeException e) {
            throw convertToDatabaseNotAvailableException(e, messages.getMessage("err.db-not-available"));
        }
    }

    /** {@inheritDoc} */
    @Override public void deleteById(String id) {
        try {
            cache().remove(id);
        }
        catch (RuntimeException e) {
            throw convertToDatabaseNotAvailableException(e, messages.getMessage("err.db-not-available"));
        }
    }
}
