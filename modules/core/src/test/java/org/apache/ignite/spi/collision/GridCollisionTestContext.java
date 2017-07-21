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

package org.apache.ignite.spi.collision;

import java.util.Collection;
import java.util.Collections;

/**
 * Tes
 */
public class GridCollisionTestContext implements CollisionContext {
    /** Active jobs. */
    private Collection<CollisionJobContext> activeJobs;

    /** Wait jobs. */
    private Collection<CollisionJobContext> waitJobs;

    /** Held jobs. */
    private Collection<CollisionJobContext> heldJobs;

    /**
     * @param activeJobs Active jobs.
     * @param waitJobs Waiting jobs.
     */
    public GridCollisionTestContext(Collection<CollisionJobContext> activeJobs,
        Collection<CollisionJobContext> waitJobs) {
        this.activeJobs = activeJobs;
        this.waitJobs = waitJobs;
    }

    /**
     * @param activeJobs Active jobs.
     * @param waitJobs Waiting jobs.
     * @param heldJobs Held jobs.
     */
    public GridCollisionTestContext(Collection<CollisionJobContext> activeJobs,
        Collection<CollisionJobContext> waitJobs,
        Collection<CollisionJobContext> heldJobs) {
        this.activeJobs = activeJobs;
        this.waitJobs = waitJobs;
        this.heldJobs = heldJobs;
    }

    /** {@inheritDoc} */
    @Override public Collection<CollisionJobContext> activeJobs() {
        return mask(activeJobs);
    }

    /** {@inheritDoc} */
    @Override public Collection<CollisionJobContext> heldJobs() {
        return mask(heldJobs);
    }

    /** {@inheritDoc} */
    @Override public Collection<CollisionJobContext> waitingJobs() {
        return mask(waitJobs);
    }

    /**
     * @param c Collection to check for {@code null}.
     * @return Non-null collection.
     */
    private Collection<CollisionJobContext> mask(Collection<CollisionJobContext> c) {
        return c == null ? Collections.<CollisionJobContext>emptyList() : c;
    }
}