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

package org.apache.ignite.cache;

import org.apache.ignite.*;

import java.util.*;

/**
 * Exception thrown from non-transactional cache in case when update succeeded only partially.
 * One can get list of keys for which update failed with method {@link #failedKeys()}.
 */
public class GridCachePartialUpdateException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Failed keys. */
    private final Collection<Object> failedKeys = new ArrayList<>();

    /**
     * @param msg Error message.
     */
    public GridCachePartialUpdateException(String msg) {
        super(msg);
    }

    /**
     * Gets collection of failed keys.
     * @return Collection of failed keys.
     */
    public <K> Collection<K> failedKeys() {
        return (Collection<K>)failedKeys;
    }

    /**
     * @param failedKeys Failed keys.
     * @param err Error.
     */
    public void add(Collection<?> failedKeys, Throwable err) {
        this.failedKeys.addAll(failedKeys);

        addSuppressed(err);
    }

    /** {@inheritDoc} */
    @Override public String getMessage() {
        return super.getMessage() + ": " + failedKeys;
    }
}
