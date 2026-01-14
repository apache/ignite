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

package org.apache.ignite.internal.management.wal;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result of WAL enable/disable operation.
 */
public class WalSetStateTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Successfully processed groups. */
    private List<String> successGrps;

    /** Errors by group name. */
    private Map<String, String> errorsByGrp;

    /** Default constructor. */
    public WalSetStateTaskResult() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param successGrps Successfully processed groups.
     * @param errorsByGrp Error messages.
     */
    public WalSetStateTaskResult(List<String> successGrps, Map<String, String> errorsByGrp) {
        this.successGrps = new ArrayList<>(successGrps);
        this.errorsByGrp = new HashMap<>(errorsByGrp);
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, successGrps);
        U.writeMap(out, errorsByGrp);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        successGrps = U.readList(in);
        errorsByGrp = U.readMap(in);
    }

    /**
     * @return Successfully processed groups.
     */
    public List<String> successGroups() {
        return Collections.unmodifiableList(successGrps);
    }

    /**
     * @return Error messages by group name if operation failed.
     */
    public Map<String, String> errorsByGroup() {
        return Collections.unmodifiableMap(errorsByGrp);
    }
}
