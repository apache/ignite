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

import java.util.concurrent.ExecutionException;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.testframework.GridTestUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Security tests for an execute server task.
 */
public class ExecuteServiceTaskTest extends AbstractContextResolverSecurityProcessorTest {
    /** */
    public void testExecute() throws Exception {
        successExecute(succsessClnt, failClnt);
        successExecute(succsessClnt, failSrv);
        successExecute(succsessSrv, failClnt);
        successExecute(succsessSrv, failSrv);
        successExecute(succsessSrv, succsessSrv);
        successExecute(succsessClnt, succsessClnt);

        failExecute(failClnt, succsessSrv);
        failExecute(failClnt, succsessClnt);
        failExecute(failSrv, succsessSrv);
        failExecute(failSrv, succsessClnt);
        failExecute(failSrv, failSrv);
        failExecute(failClnt, failClnt);
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void successExecute(IgniteEx initiator, IgniteEx remote) throws Exception {
        int val = values.getAndIncrement();

        initiator.executorService(initiator.cluster().forNode(remote.localNode()))
            .submit(
                new IgniteRunnable() {
                    @Override public void run() {
                        Ignition.localIgnite().cache(CACHE_NAME)
                            .put("key", val);
                    }
                }
            ).get();

        assertThat(remote.cache(CACHE_NAME).get("key"), is(val));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void failExecute(IgniteEx initiator, IgniteEx remote) {
        assertCauseMessage(
            GridTestUtils.assertThrowsWithCause(
                () -> {
                    try {
                        initiator.executorService(initiator.cluster().forNode(remote.localNode()))
                            .submit(
                                new IgniteRunnable() {
                                    @Override public void run() {
                                        Ignition.localIgnite().cache(CACHE_NAME).put("fail_key", -1);
                                    }
                                }
                            ).get();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                , SecurityException.class
            )
        );

        assertThat(remote.cache(CACHE_NAME).get("fail_key"), nullValue());
    }
}
