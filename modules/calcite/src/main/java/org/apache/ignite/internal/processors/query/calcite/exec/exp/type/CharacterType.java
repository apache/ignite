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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.type;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.Charset;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.commons.codec.Charsets;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/** */
public class CharacterType extends BasicType implements CharacterDataType {
    /** */
    private static final int COERCIBLE_COLLATION = 0;

    /** */
    private static final int IMPLICIT_COLLATION = 1;

    /** */
    private static final int CUSTOM_COLLATION = 2;

    /** */
    private Charset charset;

    /** */
    private SqlCollation collation;

    /** */
    public CharacterType() {
    }

    /**
     * @param type Source type.
     */
    public CharacterType(BasicSqlType type) {
        super(type);

        charset = type.getCharset();
        collation = type.getCollation();
    }

    /** {@inheritDoc} */
    @Override public Charset charset() {
        return charset;
    }

    /** {@inheritDoc} */
    @Override public SqlCollation collation() {
        return collation;
    }

    /** {@inheritDoc} */
    @Override public RelDataType logicalType(IgniteTypeFactory factory) {
        return factory.createTypeWithCharsetAndCollation(super.logicalType(factory), charset, collation);
    }

    /** {@inheritDoc} */
    @Override public Class<?> javaType(IgniteTypeFactory typeFactory) {
        return (Class<?>)typeFactory.getJavaClass(logicalType(typeFactory));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        if (charset == null)
            out.writeBoolean(false);
        else {
            out.writeBoolean(true);
            out.writeUTF(charset.name());
        }

        if (collation == SqlCollation.COERCIBLE)
            out.writeByte(COERCIBLE_COLLATION);
        else if (collation == SqlCollation.IMPLICIT)
            out.writeByte(IMPLICIT_COLLATION);
        else {
            out.writeByte(CUSTOM_COLLATION);
            out.writeUTF(collation.getCollationName());
            out.writeByte(collation.getCoercibility().ordinal());
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        if (in.readBoolean())
            charset = Charsets.toCharset(in.readUTF());

        byte collationType = in.readByte();

        if (collationType == COERCIBLE_COLLATION)
            collation = SqlCollation.COERCIBLE;
        else if (collationType == IMPLICIT_COLLATION)
            collation = SqlCollation.IMPLICIT;
        else if (collationType == CUSTOM_COLLATION)
            collation = new SqlCollation(in.readUTF(), SqlCollation.Coercibility.values()[in.readByte()]);
        else
            throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder(toString(false));

        if (charset != null)
            sb.append(" CHARACTER SET \"")
                .append(charset.name())
                .append("\"");
        if (collation != null
            && collation != SqlCollation.IMPLICIT
            && collation != SqlCollation.COERCIBLE)
            sb.append(" COLLATE \"")
                .append(collation.getCollationName())
                .append("\"");

        if (!nullable())
            sb.append(" NOT NULL");

        return sb.toString();
    }
}
