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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC client info set request.
 */
public class JdbcClientInfoSetRequest extends JdbcRequest {
    /** Set single property. */
    private boolean singleProp;

    /** Client info properties. */
    private JdbcClientInfo cliInfo;

    /**
     */
    public JdbcClientInfoSetRequest() {
        super(CLI_INFO_SET);
    }

    /**
     * @param props Client info properties.
     */
    public JdbcClientInfoSetRequest(Properties props) {
        super(CLI_INFO_SET);

        singleProp = false;
        cliInfo = new JdbcClientInfo(props);
    }

    /**
     * @param key Property key.
     * @param val Property valur.
     */
    public JdbcClientInfoSetRequest(String key, String val) {
        super(CLI_INFO_SET);

        singleProp = true;

        Properties props = new Properties();
        props.setProperty(key, val);

        cliInfo = new JdbcClientInfo(props);
    }

    /**
     * @return Fetch size.
     */
    public Properties properties() {
        return cliInfo.properties();
    }

    /**
     * @return {@code true} is just one property must be set. Otherwise returns {@code false}
     */
    public boolean isSingleProperty() {
        return singleProp;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeBoolean(singleProp);

        cliInfo.writeBinary(writer);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        singleProp = reader.readBoolean();

        cliInfo = new JdbcClientInfo();

        cliInfo.readBinary(reader);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcClientInfoSetRequest.class, this);
    }
}
