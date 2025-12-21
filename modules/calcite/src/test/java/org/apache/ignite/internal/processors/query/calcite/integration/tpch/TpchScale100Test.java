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

package org.apache.ignite.internal.processors.query.calcite.integration.tpch;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** */
public class TpchScale100Test extends AbstractTpchTest {
    @Test
    public void test() {
        // TODO Revise after https://issues.apache.org/jira/browse/IGNITE-25129
        Assumptions.assumeFalse(qryId == 16);

        super.test();
    }

    /** {@inheritDoc} */
    @Override protected double scale() {
        return 1.0;
    }
}
