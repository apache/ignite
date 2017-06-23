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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.Properties;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * JDBC client info result.
 */
public class JdbcClientInfoGetResult extends JdbcResult {
    /** Client info. */
    private JdbcClientInfo cliInfo;

    /**
     * Default Constructor.
     */
    public JdbcClientInfoGetResult() {
        super(CLI_INFO_GET );
    }

    /**
     * @param props Client info properties.
     */
    public JdbcClientInfoGetResult(Properties props) {
        super(CLI_INFO_GET );

        this.cliInfo = new JdbcClientInfo(props);
    }

    /**
     * @return Update count for DML queries.
     */
    public Properties properties() {
        return cliInfo.properties();
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        cliInfo.writeBinary(writer);
    }


    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        cliInfo = new JdbcClientInfo();

        cliInfo.readBinary(reader);
    }
}
