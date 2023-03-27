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
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result of the ScheduleIndexRebuildTask.
 */
public class ScheduleIndexRebuildTaskRes extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Map node id -> rebuild command result. */
    private Map<UUID, ScheduleIndexRebuildJobRes> results;

    /**
     * Empty constructor required for Serializable.
     */
    public ScheduleIndexRebuildTaskRes() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param results Map node id -> rebuild command result.
     */
    public ScheduleIndexRebuildTaskRes(Map<UUID, ScheduleIndexRebuildJobRes> results) {
        this.results = results;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, results);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        results = U.readMap(in);
    }

    /**
     * @return Map node id -> rebuild command result.
     */
    public Map<UUID, ScheduleIndexRebuildJobRes> results() {
        return results;
    }
}
