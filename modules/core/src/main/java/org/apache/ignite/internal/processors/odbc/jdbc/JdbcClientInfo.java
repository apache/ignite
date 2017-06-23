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

import java.util.Map;
import java.util.Properties;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * JDBC client info properties.
 */
public class JdbcClientInfo implements JdbcRawBinarylizable {
    /** Client info properties. */
    private Properties props;

    /**
     * Default constructor is used for serialization.
     */
    public JdbcClientInfo() {
    }

    /**
     * @param props Client info properties.
     */
    public JdbcClientInfo(Properties props) {
        this.props = props;
    }

    /**
     * @return Fetch size.
     */
    public Properties properties() {
        return props;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) {
        if (props != null) {
            writer.writeInt(props.size());

            for (Map.Entry<Object, Object> e : props.entrySet()) {
                writer.writeString((String)e.getKey());
                writer.writeString((String)e.getValue());
            }
        }
        else
            writer.writeInt(0);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) {
        int propNum = reader.readInt();

        if (propNum == 0)
            return;

        props = new Properties();

        for (int i = 0; i < propNum; ++i) {
            String name = reader.readString();
            String val = reader.readString();

            props.setProperty(name, val);
        }
    }
}
