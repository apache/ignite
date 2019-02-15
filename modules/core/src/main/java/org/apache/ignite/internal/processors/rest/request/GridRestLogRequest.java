/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.rest.request;

import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Grid command request of log file.
 */
public class GridRestLogRequest extends GridRestRequest {
    /** Task name. */
    private String path;

    /** From line, inclusive, indexing from 0. */
    private int from = -1;

    /** To line, inclusive, indexing from 0, can exceed count of lines in log. */
    private int to = -1;

    /**
     * Constructor.
     */
    public GridRestLogRequest() {
        command(GridRestCommand.LOG);
    }

    /**
     * @return Path to log file.
     */
    public String path() {
        return path;
    }

    /**
     * @param path Path to log file.
     */
    public void path(String path) {
        this.path = path;
    }

    /**
     * @return From line, inclusive, indexing from 0.
     */
    public int from() {
        return from;
    }

    /**
     * @param from From line, inclusive, indexing from 0.
     */
    public void from(int from) {
        this.from = from;
    }

    /**
     * @return To line, inclusive, indexing from 0.
     */
    public int to() {
        return to;
    }

    /**
     * @param to To line, inclusive, indexing from 0.
     */
    public void to(int to) {
        this.to = to;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestLogRequest.class, this, super.toString());
    }
}
