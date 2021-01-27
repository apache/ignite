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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.nio.file.Path;
import org.apache.ignite.internal.GridKernalContext;

/**
 * Interface to check snapshot files integrity before restoring them.
 */
public interface SnapshotDigest {
    /**
     * @return Instance of SnapshotDigest.
     */
    public static SnapshotDigest getInstance(GridKernalContext ctx) {
        SnapshotDigest res = ctx.plugins().createComponent(SnapshotDigest.class);

        if (res == null) {
            res = new SnapshotDigest() {
                @Override public void verify(Path snpDir) {
                    // No-op.
                }
            };
        }

        return res;
    }

    /**
     * Verify snapshot integrity.
     *
     * @param snpDir Path to a snapshot directory.
     * @throws SnapshotDigestException Thrown if snapshot integrity is corrupted or impossible to check.
     */
    public void verify(Path snpDir) throws SnapshotDigestException;

}
