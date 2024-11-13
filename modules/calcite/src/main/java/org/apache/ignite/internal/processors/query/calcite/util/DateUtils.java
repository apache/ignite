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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.math.BigDecimal;

import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.MILLIS_PER_DAY;
import static org.apache.ignite.internal.processors.query.calcite.util.IgniteMath.convertToBigDecimal;
import static org.apache.ignite.internal.processors.query.calcite.util.IgniteMath.round;

/** */
public class DateUtils {
    /** */
    public static long oracleMillis(Number val) {
        BigDecimal raw;

        if (val instanceof BigDecimal)
            raw = (BigDecimal)val;
        else
            raw = convertToBigDecimal(val);

        raw = raw.multiply(BigDecimal.valueOf(MILLIS_PER_DAY));

        return round(raw).longValue();
    }
}
