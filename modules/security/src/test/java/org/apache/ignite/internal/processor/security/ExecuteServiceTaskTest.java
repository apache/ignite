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

public class ExecuteServiceTaskTest extends AbstractInintiatorContextSecurityProcessorTest {
    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void successExecute(IgniteEx initiator, IgniteEx remote, String key) {
        initiator.executorService(initiator.cluster().forNode(remote.localNode()))
            .submit(
                ()->
                    Ignition.localIgnite().cache(CACHE_NAME).put(key, "value")
            );

        assertThat(remote.cache(CACHE_NAME).get(key), is("value"));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void failExecute(IgniteEx initiator, IgniteEx remote, String key) {
        assertCauseMessage(
            GridTestUtils.assertThrowsWithCause(
                () -> initiator.executorService(initiator.cluster().forNode(remote.localNode()))
                    .submit(
                        () ->
                            Ignition.localIgnite().cache(CACHE_NAME).put(key, "value")
                    )
                , SecurityException.class
            )
        );

        assertThat(remote.cache(CACHE_NAME).get(key), nullValue());
    }

}
