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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.UUID;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class MvccFuture<T> extends GridFutureAdapter<T> {
    /** */
    protected UUID crdId;

    /**
     * Default constructor.
     */
    public MvccFuture() {
    }

    /**
     * @param crdId MVCC coordinator node ID.
     */
    public MvccFuture(UUID crdId) {
        assert crdId != null;

        this.crdId = crdId;
    }

    /**
     * @return MVCC coordinator node ID.
     */
    public UUID coordinatorNodeId() {
        return crdId;
    }

    /**
     * @param crdId MVCC coordinator node ID.
     */
    public void coordinatorNodeId(UUID crdId) {
        assert crdId != null;

        this.crdId = crdId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccFuture.class, this, super.toString());
    }
}
