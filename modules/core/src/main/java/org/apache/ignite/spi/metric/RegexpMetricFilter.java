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

package org.apache.ignite.spi.metric;

import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Metric registry filter based on regular expression.
 */
@IgniteExperimental
public class RegexpMetricFilter implements Predicate<ReadOnlyMetricRegistry> {
    /** Metric registries pattern. */
    private final Pattern regPtrn;

    /**
     * @param regNameRegex Regular expression to filter metric registries.
     */
    public RegexpMetricFilter(String regNameRegex) {
        A.notNull(regNameRegex, "regex");

        regPtrn = Pattern.compile(regNameRegex);
    }

    /** {@inheritDoc} */
    @Override public boolean test(ReadOnlyMetricRegistry mreg) {
        Matcher m = regPtrn.matcher(mreg.name());

        return m.matches();
    }
}
