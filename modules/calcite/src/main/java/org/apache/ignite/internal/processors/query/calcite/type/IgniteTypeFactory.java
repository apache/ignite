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

package org.apache.ignite.internal.processors.query.calcite.type;

import java.nio.charset.Charset;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeUtil;

/**
 * Ignite type factory.
 */
public class IgniteTypeFactory extends JavaTypeFactoryImpl {
    /** */
    public IgniteTypeFactory() {
        super(IgniteTypeSystem.INSTANCE);
    }

    /**
     * @param typeSystem Type system.
     */
    public IgniteTypeFactory(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }

    /**
     * Creates a system type.
     */
    public RelDataType createSystemType(Class<?> clazz) {
        final SystemJavaType javaType = clazz != String.class ? new SystemJavaType(clazz)
            : new SystemJavaType(clazz, true, getDefaultCharset(), SqlCollation.IMPLICIT);

        return canonize(javaType);
    }

    /** {@inheritDoc} */
    @Override public RelDataType createTypeWithCharsetAndCollation(RelDataType type, Charset charset, SqlCollation collation) {
        if (type instanceof SystemJavaType)
            return canonize(((SystemJavaType) type).copyWithCharsetAndCollation(charset, collation));

        return super.createTypeWithCharsetAndCollation(type, charset, collation);
    }

    /** {@inheritDoc} */
    @Override public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
        if (type instanceof SystemJavaType)
            return canonize(((SystemJavaType) type).copyWithNullability(nullable));

        return super.createTypeWithNullability(type, nullable);
    }

    /** */
    public class SystemJavaType extends JavaType implements SystemType {
        /**
         * @param clazz Java type.
         */
        public SystemJavaType(Class<?> clazz) {
            super(clazz);
        }

        /**
         * @param clazz Java type.
         * @param nullable Nullability.
         */
        public SystemJavaType(Class<?> clazz, boolean nullable) {
            super(clazz, nullable);
        }

        /**
         * @param clazz Java type.
         * @param nullable Nullability.
         * @param charset Charset.
         * @param collation Collation.
         */
        public SystemJavaType(Class<?> clazz, boolean nullable, Charset charset, SqlCollation collation) {
            super(clazz, nullable, charset, collation);
        }

        /** {@inheritDoc} */
        @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
            sb.append("SystemJavaType(") .append(getJavaClass()) .append(")");
        }

        /** */
        private SystemJavaType copyWithCharsetAndCollation(Charset charset, SqlCollation collation) {
            return new SystemJavaType(getJavaClass(), isNullable(), charset, collation);
        }

        /** */
        private SystemJavaType copyWithNullability(boolean nullable) {
            return SqlTypeUtil.inCharFamily(this) ? new SystemJavaType(getJavaClass(), nullable, getCharset(), getCollation())
                : new SystemJavaType(nullable ? Primitive.box(getJavaClass()) : Primitive.unbox(getJavaClass()), nullable);
        }
    }
}
