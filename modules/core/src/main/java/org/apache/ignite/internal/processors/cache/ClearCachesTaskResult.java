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

package org.apache.ignite.internal.processors.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/** Result of {@link ClearCachesTask}. */
public class ClearCachesTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** List of cleared caches. */
    private List<String> clearedCaches;

    /** List of non-existent caches. */
    private List<String> nonExistentCaches;

    /** */
    public ClearCachesTaskResult(List<String> clearedCaches, List<String> nonExistentCaches) {
        this.clearedCaches = clearedCaches;
        this.nonExistentCaches = nonExistentCaches;
    }

    /** */
    public ClearCachesTaskResult() {
        // No-op.
    }

    /** @return List of cleared caches. */
    public List<String> clearedCaches() {
        return clearedCaches;
    }

    /** @return List of non-existent caches. */
    public List<String> nonExistentCaches() {
        return nonExistentCaches;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, clearedCaches);
        U.writeCollection(out, nonExistentCaches);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        clearedCaches = U.readList(in);
        nonExistentCaches = U.readList(in);
    }
}
