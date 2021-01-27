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

package org.apache.ignite.cdc;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** */
public class UtilsSelfTest {
    /** */
    @Test
    public void testPartitionsParse() {
        assertThat(Utils.partitions("1,2,3,42-45"), is(new HashSet<>(Arrays.asList(1, 2, 3, 42, 43, 44, 45))));

        assertThat(Utils.partitions("1,2,3"), is(new HashSet<>(Arrays.asList(1, 2, 3))));

        assertThat(Utils.partitions("1"), is(new HashSet<>(Collections.singletonList(1))));

        assertThat(Utils.partitions("42-45"), is(new HashSet<>(Arrays.asList(42, 43, 44, 45))));

        assertThat(Utils.partitions("42-45,47-47"), is(new HashSet<>(Arrays.asList(42, 43, 44, 45, 47))));
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testPartitionsParseError() {
        Utils.partitions("");
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testPartitionsParseError2() {
        Utils.partitions("45-42");
    }
}