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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * JDBC tables metadata result.
 */
public class JdbcMetaSchemasResult extends JdbcResult {
    /** Query result rows. */
    private List<String> schemas;

    /**
     * Default constructor is used for deserialization.
     */
    JdbcMetaSchemasResult() {
        super(META_SCHEMAS);
    }

    /**
     * @param schemas Found schemas.
     */
    JdbcMetaSchemasResult(List<String> schemas) {
        super(META_SCHEMAS);
        this.schemas = schemas;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeStringArray(schemas.toArray(new String[schemas.size()]));
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        schemas = new ArrayList<>(Arrays.asList(reader.readStringArray()));
    }

    /**
     * @return Query result rows.
     */
    public List<String> schemas() {
        return schemas;
    }
}
