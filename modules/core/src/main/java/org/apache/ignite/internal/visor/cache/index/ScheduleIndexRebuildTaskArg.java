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

package org.apache.ignite.internal.visor.cache.index;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Argument for the ScheduleIndexRebuildTask.
 */
public class ScheduleIndexRebuildTaskArg extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Map cache names -> indexes scheduled for the rebuild. */
    private Map<String, Set<String>> cacheToIndexes;

    /** Set of cache group names scheduled for the rebuild. */
    private Set<String> cacheGroups;

    /**
     * Empty constructor required for Serializable.
     */
    public ScheduleIndexRebuildTaskArg() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheToIndexes Map cache names -> indexes scheduled for the rebuild.
     * @param cacheGroups Cache group names scheduled for the rebuild.
     */
    public ScheduleIndexRebuildTaskArg(Map<String, Set<String>> cacheToIndexes, Set<String> cacheGroups) {
        this.cacheToIndexes = cacheToIndexes;
        this.cacheGroups = cacheGroups;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, cacheToIndexes);
        U.writeCollection(out, cacheGroups);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheToIndexes = U.readMap(in);
        cacheGroups = U.readSet(in);
    }

    /**
     * @return Map cache names -> indexes scheduled for the rebuild.
     */
    public Map<String, Set<String>> cacheToIndexes() {
        return cacheToIndexes;
    }

    /**
     * @return Set of cache group names scheduled for the rebuild.
     */
    public Set<String> cacheGroups() {
        return cacheGroups;
    }
}
