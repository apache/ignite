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

package org.apache.ignite.internal.processors.cache.persistence.tree.reuse;

import java.io.Externalizable;
import org.apache.ignite.internal.util.GridLongList;

/**
 * {@link GridLongList}-based reuse bag.
 */
public final class LongListReuseBag extends GridLongList implements ReuseBag {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor for {@link Externalizable}.
     */
    public LongListReuseBag() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void addFreePage(long pageId) {
        add(pageId);
    }

    /** {@inheritDoc} */
    @Override public long pollFreePage() {
        return isEmpty() ? 0 : remove();
    }
}
