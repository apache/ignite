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

package org.apache.ignite.internal.processor.security;

import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Security tests for a compute task.
 */
public class ComputeTaskTest extends AbstractInintiatorContextSecurityProcessorTest {
    /** */
    public void testCompute() {
        successCompute(succsessClnt, failClnt, "10");

        successCompute(succsessClnt, failSrv, "20");

        successCompute(succsessSrv, failClnt, "30");

        successCompute(succsessSrv, failSrv, "40");

        successCompute(succsessSrv, succsessSrv, "50");

        successCompute(succsessClnt, succsessClnt, "60");

        failCompute(failClnt, succsessSrv, "70");

        failCompute(failClnt, succsessClnt, "80");

        failCompute(failSrv, succsessSrv, "90");

        failCompute(failSrv, succsessClnt, "100");

        failCompute(failSrv, failSrv, "110");

        failCompute(failClnt, failClnt, "120");
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void successCompute(IgniteEx initiator, IgniteEx remote, String key) {
        initiator.compute(initiator.cluster().forNode(remote.localNode()))
            .broadcast(() ->
                Ignition.localIgnite().cache(CACHE_NAME).put(key, "value")
            );

        assertThat(remote.cache(CACHE_NAME).get(key), is("value"));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void failCompute(IgniteEx initiator, IgniteEx remote, String key) {
        assertCauseMessage(
            GridTestUtils.assertThrowsWithCause(
                () -> initiator.compute(initiator.cluster().forNode(remote.localNode()))
                    .broadcast(() ->
                        Ignition.localIgnite().cache(CACHE_NAME).put(key, "value")
                    )
                , SecurityException.class
            )
        );

        assertThat(remote.cache(CACHE_NAME).get(key), nullValue());
    }
}