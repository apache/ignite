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
package org.apache.ignite.internal.processors.query.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

/**
 * A SQL type name specification of interval types.
 */
public class IgniteSqlIntervalTypeNameSpec extends SqlTypeNameSpec {
    /** */
    private final SqlIntervalQualifier sqlIntervalQualifier;

    /** Constructor. */
    public IgniteSqlIntervalTypeNameSpec(SqlIntervalQualifier intervalQualifier, SqlParserPos pos) {
        super(new SqlIdentifier(intervalQualifier.typeName().name(), pos), pos);
        sqlIntervalQualifier = intervalQualifier;
    }

    /** {@inheritDoc} */
    @Override public RelDataType deriveType(SqlValidator validator) {
        return validator.getTypeFactory().createSqlIntervalType(sqlIntervalQualifier);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("INTERVAL");

        getTypeName().unparse(writer, leftPrec, rightPrec);
    }

    /** {@inheritDoc} */
    @Override public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
        if (!(spec instanceof IgniteSqlIntervalTypeNameSpec))
            return false;

        IgniteSqlIntervalTypeNameSpec that = (IgniteSqlIntervalTypeNameSpec)spec;

        return getTypeName().equalsDeep(that.getTypeName(), litmus);
    }
}
