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

package org.apache.ignite.java8.examples;

import org.apache.ignite.examples.java8.messaging.MessagingExample;
import org.apache.ignite.examples.java8.messaging.MessagingPingPongExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;

/**
 * Messaging examples self test.
 */
public class MessagingExamplesSelfTest extends GridAbstractExamplesTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid("companion", DFLT_CFG);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMessagingExample() throws Exception {
        MessagingExample.main(EMPTY_ARGS);
    }

    /**
     * TODO IGNITE-533
     * @throws Exception If failed.
     */
    public void testMessagingPingPongExample() throws Exception {
        MessagingPingPongExample.main(EMPTY_ARGS);
    }

//    TODO: IGNITE-711 next example(s) should be implemented for java 8
//    or testing method(s) should be removed if example(s) does not applicable for java 8.
//    /**
//     * @throws Exception If failed.
//     */
//    public void testMessagingPingPongListenActorExample() throws Exception {
//        MessagingPingPongListenActorExample.main(EMPTY_ARGS);
//    }
}
