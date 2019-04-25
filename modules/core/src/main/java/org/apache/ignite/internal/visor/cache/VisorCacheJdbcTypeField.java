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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for {@link JdbcTypeField}.
 */
public class VisorCacheJdbcTypeField extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Column name in database. */
    private String dbName;

    /** Column JDBC type in database. */
    private int dbType;

    /** Field name in java object. */
    private String javaName;

    /** Corresponding java type. */
    private String javaType;

    /**
     * Empty constructor.
     */
    public VisorCacheJdbcTypeField() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param dbName Column name in database.
     * @param dbType Column JDBC type in database.
     * @param javaName Field name in java object.
     * @param javaType Corresponding java type.
     */
    public VisorCacheJdbcTypeField(String dbName, int dbType, String javaName, String javaType) {
        this.dbName = dbName;
        this.dbType = dbType;
        this.javaName = javaName;
        this.javaType = javaType;
    }

    /**
     * @return Column name in database.
     */
    public String getDatabaseName() {
        return dbName;
    }

    /**
     * @return Column JDBC type in database.
     */
    public int getDatabaseType() {
        return dbType;
    }

    /**
     * @return Field name in java object.
     */
    public String getJavaName() {
        return javaName;
    }

    /**
     * @return Corresponding java type.
     */
    public String getJavaType() {
        return javaType;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, dbName);
        out.writeInt(dbType);
        U.writeString(out, javaName);
        U.writeString(out, javaType);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        dbName = U.readString(in);
        dbType = in.readInt();
        javaName = U.readString(in);
        javaType = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheJdbcTypeField.class, this);
    }
}
