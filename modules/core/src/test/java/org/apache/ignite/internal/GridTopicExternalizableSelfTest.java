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

package org.apache.ignite.internal;

import java.io.Externalizable;
import java.util.UUID;
import org.apache.ignite.GridTestIoUtils;
import org.apache.ignite.IgniteExternalizableAbstractTest;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;

/**
 * Grid topic externalization test.
 */
public class GridTopicExternalizableSelfTest extends IgniteExternalizableAbstractTest {
    /** */
    private static final IgniteUuid A_GRID_UUID = IgniteUuid.randomUuid();

    /** */
    private static final UUID AN_UUID = UUID.randomUUID();

    /** */
    private static final long A_LONG = Long.MAX_VALUE;

    /** */
    private static final String A_STRING = "test_test_test_test_test_test_test_test_test_test_test_test_test_test";

    /** */
    private static final int AN_INT = Integer.MAX_VALUE;

    /**
     * @throws Exception If failed.
     */
    public void testSerializationTopicCreatedByGridUuid() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_GRID_UUID);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerializationTopicCreatedByGridUuidAndUUID() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_GRID_UUID, AN_UUID);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerializationTopicCreatedByGridUuidAndLong() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_GRID_UUID, A_LONG);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerializationTopicCreatedByStringAndUUIDAndLong() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_STRING, AN_UUID, A_LONG);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerializationTopicCreatedByString() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_STRING);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerializationTopicCreatedByStringAndIntAndLong() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_STRING, AN_INT, A_LONG);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerializationTopicCreatedByStrinAndLong() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_STRING, A_LONG);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerializationTopicCreatedByStringAndUUIDAndIntAndLong() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_STRING, AN_UUID, AN_INT, A_LONG);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }
}