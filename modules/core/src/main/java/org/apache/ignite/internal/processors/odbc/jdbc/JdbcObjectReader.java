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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.odbc.SqlListenerAbstractObjectReader;
import org.jetbrains.annotations.Nullable;

/**
 * Binary reader with marshaling non-primitive and non-embedded objects with JDK marshaller.
 */
@SuppressWarnings("unchecked")
public class JdbcObjectReader extends SqlListenerAbstractObjectReader {
    /**
     * @param reader Binary reader,
     * @return SQL object.
     * @throws BinaryObjectException On error.
     */
    @Nullable public Object readSqlObject(BinaryReaderExImpl reader) throws BinaryObjectException {
        Object obj = readObject(reader);

        if (obj == null)
            return null;

        if (obj.getClass() == java.util.Date.class)
            return new java.sql.Date(((java.util.Date)obj).getTime());

        if (obj.getClass() == java.util.Date[].class) {
            java.util.Date[] arr = (java.util.Date[])obj;

            java.sql.Date[] sqlArr = new java.sql.Date[arr.length];

            for (int i = 0; i < arr.length; ++i)
                sqlArr[i] = new java.sql.Date(arr[i].getTime());

            return sqlArr;
        }

        return obj;
    }

    /** {@inheritDoc} */
    @Override protected Object readCustomObject(BinaryReaderExImpl reader) throws BinaryObjectException {
        throw new BinaryObjectException("JDBC doesn't support not embedded objects.");
    }
}
