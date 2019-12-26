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

import java.io.Serializable;
import java.util.Set;
import org.springframework.session.ExpiringSession;
import org.springframework.session.MapSession;
import org.springframework.util.Assert;

/**
 * Ignite session wrapper.
 */
public final class IgniteSession implements ExpiringSession, Serializable {
    /** Delegate session. */
    private final MapSession delegate;

    /** Session original id. */
    private String originalId;

    /** Empty constructor. */
    public IgniteSession() {
        this(new MapSession());
    }

    /**
     * Constructor.
     *
     * @param ses Map session delegate item.
     */
    public IgniteSession(MapSession ses) {
        Assert.notNull(ses, "Already cached session cannot be null");
        this.delegate = ses;
        this.originalId = ses.getId();
    }

    /** Gets id. */
    public String getId() {
        return this.delegate.getId();
    }

    /** /** {@inheritDoc} */
    @Override
    public <T> T getAttribute(String attributeName) {
        return this.delegate.getAttribute(attributeName);
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> getAttributeNames() {
        return this.delegate.getAttributeNames();
    }

    /** {@inheritDoc} */
    @Override
    public void setAttribute(String attributeName, Object attributeValue) {
        this.delegate.setAttribute(attributeName, attributeValue);
    }

    /** {@inheritDoc} */
    @Override
    public void removeAttribute(String attributeName) {
        this.delegate.removeAttribute(attributeName);
    }

    /** {@inheritDoc} */
    @Override
    public long getCreationTime() {
        return this.delegate.getCreationTime();
    }

    /** {@inheritDoc} */
    @Override
    public void setLastAccessedTime(long lastAccessedTime) {
        this.delegate.setLastAccessedTime(lastAccessedTime);
    }

    /** {@inheritDoc} */
    @Override
    public long getLastAccessedTime() {
        return this.delegate.getLastAccessedTime();
    }

    /** {@inheritDoc} */
    @Override
    public void setMaxInactiveIntervalInSeconds(int interval) {
        this.delegate.setMaxInactiveIntervalInSeconds(interval);
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxInactiveIntervalInSeconds() {
        return this.delegate.getMaxInactiveIntervalInSeconds();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isExpired() {
        return this.delegate.isExpired();
    }

    /**
     * Sets original id.
     *
     * @param originalId Original id.
     */
    public void setOriginalId(String originalId) {
        this.originalId = originalId;
    }

    /**
     * Gets original id.
     *
     * @return Original id.
     */
    public String getOriginalId() {
        return originalId;
    }
}