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

package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteProductVersion;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.DATA_STREAM_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;

/**
 * Default IO policy resolver.
 */
class DefaultIoPolicyResolver implements IgniteClosure<ClusterNode, Byte> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Data streamer separate pool feature major version. */
    private static final int DATA_STREAMER_POOL_MAJOR_VER = 1;

    /** Data streamer separate pool feature minor version. */
    private static final int DATA_STREAMER_POOL_MINOR_VER = 7;

    /** Data streamer separate pool feature maintenance version. */
    private static final int DATA_STREAMER_POOL_MAINT_VER = 3;

    /** {@inheritDoc} */
    @Override public Byte apply(ClusterNode gridNode) {
        assert gridNode != null;

        IgniteProductVersion version = gridNode.version();

        //TODO: change version to actual before merge.
        if (gridNode.isLocal() || version.greaterThanEqual(DATA_STREAMER_POOL_MAJOR_VER, DATA_STREAMER_POOL_MINOR_VER,
            DATA_STREAMER_POOL_MAINT_VER))
            return DATA_STREAM_POOL;
        else
            return PUBLIC_POOL;
    }
}
