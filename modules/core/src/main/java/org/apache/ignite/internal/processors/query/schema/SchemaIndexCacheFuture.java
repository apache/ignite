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

package org.apache.ignite.internal.processors.query.schema;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Extending {@link GridFutureAdapter} to rebuild indices.
 */
public class SchemaIndexCacheFuture extends GridFutureAdapter<Void> {
    /** Token for canceling index rebuilding.*/
    private final AtomicReference<Throwable> cancelTok;

    /**
     * Constructor.
     *
     * @param cancelTok Token for canceling index rebuilding.
     */
    public SchemaIndexCacheFuture(AtomicReference<Throwable> cancelTok) {
        this.cancelTok = cancelTok;
    }

    /**
     * @return {@code true} if cancellation is in progress.
     */
    public boolean hasTokenException() {
        return cancelTok.get() != null;
    }

    /**
     * @return {@code true} if token status set by this call.
     */
    public boolean setTokenException(IgniteCheckedException e) {
        return cancelTok.compareAndSet(null, e);
    }
}
