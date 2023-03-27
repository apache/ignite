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
package org.apache.ignite.internal.processors.cache.persistence.evict;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;

/**
 *
 */
public class NoOpPageEvictionTracker implements PageEvictionTracker {
    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void touchPage(long pageId) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void evictDataPage() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void forgetPage(long pageId) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean evictionRequired() {
        return false;
    }
}
