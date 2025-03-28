/*
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 */

package org.apache.ignite.internal.management.wal;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *  Result of {@link WalTask}.
 */
public class WalTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Exceptions by node consistent id. */
    @GridToStringInclude
    private Map<String, Exception> exceptions;

    /** Archived wal segments path search results by node consistent id. */
    @GridToStringInclude
    private Map<String, Collection<String>> results;

    /** Nodes info by node consistent id. */
    @GridToStringInclude
    private Map<String, ClusterNode> nodesInfo;

    /**
     * Default constructor.
     */
    public WalTaskResult() {
        // No-op.
    }

    /**
     * Create {@link WalTask } result with given parameters.
     *
     * @param results List of log search results.
     * @param exceptions List of exceptions by node id.
     * @param nodesInfo Nodes info.
     */
    public WalTaskResult(Map<String, Collection<String>> results, Map<String, Exception> exceptions,
                              Map<String, ClusterNode> nodesInfo) {
        this.exceptions = exceptions;
        this.results = results;
        this.nodesInfo = nodesInfo;
    }

    /**
     *  Get occurred errors by node consistent id.
     *
     * @return Exceptions by node consistent id.
     */
    public Map<String, Exception> exceptions() {
        return exceptions;
    }

    /**
     * @return List of archived wal segments path search results by node consistent id.
     */
    public Map<String, Collection<String>> results() {
        return results;
    }

    /**
     * Get nodes info by node consistent id.
     *
     * @return Nodes info.
     */
    public Map<String, ClusterNode> getNodesInfo() {
        return nodesInfo;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, exceptions);
        U.writeMap(out, results);
        U.writeMap(out, nodesInfo);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        exceptions = U.readMap(in);
        results = U.readMap(in);
        nodesInfo = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WalTaskResult.class, this);
    }
}
