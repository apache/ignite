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

package org.apache.ignite.internal.processors.query.calcite.rules;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

import static org.junit.Assert.fail;

/**
 * Plan checker.
 */
class PlanMatcher {
    /**
     * IgniteScan matcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    static Matcher<String> containsScan(String schema, String tblName) {
        return CoreMatchers.containsString("IgniteTableScan(table=[[" + schema + ", " + tblName + "]]");
    }

    /**
     * IgniteScan matcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @return Matcher.
     */
    static Matcher<String> containsScan(String schema, String tblName, String idxName) {
        return CoreMatchers.containsString("IgniteTableScan(table=[[" + schema + ", " + tblName + "]], index=[" + idxName + ']');
    }

    /**
     * UnionAll checker.
     *
     * @return Matcher.
     */
    static Matcher<String> containsUnionAll() {
        return CoreMatchers.containsString("IgniteUnionAll(all=[true])");
    }

    /** Plan matcher. */
    private final Matcher<String> matcher;

    /**
     * Constructor.
     *
     * @param matcher Query plan matcher.
     */
    public PlanMatcher(Matcher<String> matcher) {
        this.matcher = matcher;
    }

    /**
     * Check query plan.
     *
     * @param plan Query plan.
     */
    public void check(String plan) {
        if (!matcher.matches(plan)) {
            final StringDescription desc = new StringDescription();

            matcher.describeTo(desc);

            fail(desc.toString());
        }
    }
}
