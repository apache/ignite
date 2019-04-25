/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.internal.jdbc.thin.JdbcThinTcpIo;

/**
 * Jdbc result with IO.
 */
public final class JdbcResultWithIo {
    /** JDBC response result. */
    private final JdbcResult res;

    /** Sticky cliIo. */
    private final JdbcThinTcpIo cliIo;

    /**
     * Constructor.
     *
     * @param res JDBC response result.
     * @param cliIo Ignite endpoint.
     */
    public JdbcResultWithIo(JdbcResult res, JdbcThinTcpIo cliIo) {
        this.res = res;
        this.cliIo = cliIo;
    }

    /**
     * @return Response.
     */
    public <R extends JdbcResult> R response() {
        return (R) res;
    }

    /**
     * @return Cli io.
     */
    public JdbcThinTcpIo cliIo() {
        return cliIo;
    }
}
