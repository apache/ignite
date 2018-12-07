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

package org.apache.ignite.internal.processors.query.h2.dml;

import java.lang.reflect.Array;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.util.DateTimeUtils;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * DML utility methods.
 */
public class DmlUtils {
    /**
     * Convert value to column's expected type by means of H2.
     *
     * @param val Source value.
     * @param desc Row descriptor.
     * @param expCls Expected value class.
     * @param type Expected column type to convert to.
     * @return Converted object.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"ConstantConditions", "SuspiciousSystemArraycopy"})
    public static Object convert(Object val, GridH2RowDescriptor desc, Class<?> expCls, int type)
        throws IgniteCheckedException {
        if (val == null)
            return null;

        Class<?> currCls = val.getClass();

        try {
            if (val instanceof Date && currCls != Date.class && expCls == Date.class) {
                // H2 thinks that java.util.Date is always a Timestamp, while binary marshaller expects
                // precise Date instance. Let's satisfy it.
                return new Date(((Date) val).getTime());
            }

            // User-given UUID is always serialized by H2 to byte array, so we have to deserialize manually
            if (type == Value.UUID && currCls == byte[].class)
                return U.unmarshal(desc.context().marshaller(), (byte[]) val,
                    U.resolveClassLoader(desc.context().gridConfig()));

            if (LocalDateTimeUtils.isJava8DateApiPresent()) {
                if (val instanceof Timestamp && LocalDateTimeUtils.LOCAL_DATE_TIME ==expCls)
                    return LocalDateTimeUtils.valueToLocalDateTime(ValueTimestamp.get((Timestamp) val));

                if (val instanceof Date && LocalDateTimeUtils.LOCAL_DATE == expCls)
                    return LocalDateTimeUtils.valueToLocalDate(ValueDate.fromDateValue(
                        DateTimeUtils.dateValueFromDate(((Date) val).getTime())));

                if (val instanceof Time && LocalDateTimeUtils.LOCAL_TIME == expCls)
                    return LocalDateTimeUtils.valueToLocalTime(ValueTime.get((Time) val));
            }

            // We have to convert arrays of reference types manually -
            // see https://issues.apache.org/jira/browse/IGNITE-4327
            // Still, we only can convert from Object[] to something more precise.
            if (type == Value.ARRAY && currCls != expCls) {
                if (currCls != Object[].class)
                    throw new IgniteCheckedException("Unexpected array type - only conversion from Object[] " +
                        "is assumed");

                // Why would otherwise type be Value.ARRAY?
                assert expCls.isArray();

                Object[] curr = (Object[]) val;

                Object newArr = Array.newInstance(expCls.getComponentType(), curr.length);

                System.arraycopy(curr, 0, newArr, 0, curr.length);

                return newArr;
            }

            Object res = H2Utils.convert(val, desc, type);

            if (res instanceof Date && res.getClass() != Date.class && expCls == Date.class) {
                // We can get a Timestamp instead of Date when converting a String to Date
                // without query - let's handle this
                return new Date(((Date) res).getTime());
            }

            return res;
        }
        catch (Exception e) {
            throw new IgniteSQLException("Value conversion failed [from=" + currCls.getName() + ", to=" +
                expCls.getName() +']', IgniteQueryErrorCode.CONVERSION_FAILED, e);
        }
    }

    /**
     * Check whether query is batched.
     *
     * @param qry Query.
     * @return {@code True} if batched.
     */
    public static boolean isBatched(SqlFieldsQuery qry) {
        return (qry instanceof SqlFieldsQueryEx) && ((SqlFieldsQueryEx)qry).isBatched();
    }

    /**
     * Private constructor.
     */
    private DmlUtils() {
        // No-op.
    }
}
