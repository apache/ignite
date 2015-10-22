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

package org.apache.ignite.platform;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test task producing result without any arguments.
 */
public class PlatformComputeEchoTask extends ComputeTaskAdapter<Integer, Object> {
    /** Type: NULL. */
    private static final int TYPE_NULL = 0;

    /** Type: byte. */
    private static final int TYPE_BYTE = 1;

    /** Type: bool. */
    private static final int TYPE_BOOL = 2;

    /** Type: short. */
    private static final int TYPE_SHORT = 3;

    /** Type: char. */
    private static final int TYPE_CHAR = 4;

    /** Type: int. */
    private static final int TYPE_INT = 5;

    /** Type: long. */
    private static final int TYPE_LONG = 6;

    /** Type: float. */
    private static final int TYPE_FLOAT = 7;

    /** Type: double. */
    private static final int TYPE_DOUBLE = 8;

    /** Type: array. */
    private static final int TYPE_ARRAY = 9;

    /** Type: collection. */
    private static final int TYPE_COLLECTION = 10;

    /** Type: map. */
    private static final int TYPE_MAP = 11;

    /** Type: portable object which exists in all platforms. */
    private static final int TYPE_PORTABLE = 12;

    /** Type: portable object which exists only in Java. */
    private static final int TYPE_PORTABLE_JAVA = 13;

    /** Type: object array. */
    private static final int TYPE_OBJ_ARRAY = 14;

    /** Type: portable object array. */
    private static final int TYPE_PORTABLE_ARRAY = 15;

    /** Type: enum. */
    private static final int TYPE_ENUM = 16;

    /** Type: enum array. */
    private static final int TYPE_ENUM_ARRAY = 17;

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Integer arg) {
        return Collections.singletonMap(new EchoJob(arg), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) {
        return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class EchoJob extends ComputeJobAdapter {
        /** Type. */
        private Integer type;

        /**
         * Constructor.
         *
         * @param type Result type.
         */
        public EchoJob(Integer type) {
            this.type = type;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            switch (type) {
                case TYPE_NULL:
                    return null;

                case TYPE_BYTE:
                    return (byte)1;

                case TYPE_BOOL:
                    return true;

                case TYPE_SHORT:
                    return (short)1;

                case TYPE_CHAR:
                    return (char)1;

                case TYPE_INT:
                    return 1;

                case TYPE_LONG:
                    return (long)1;

                case TYPE_FLOAT:
                    return (float)1;

                case TYPE_DOUBLE:
                    return (double)1;

                case TYPE_ARRAY:
                    return new int[] { 1 };

                case TYPE_COLLECTION:
                    return Collections.singletonList(1);

                case TYPE_MAP:
                    return Collections.singletonMap(1, 1);

                case TYPE_PORTABLE:
                    return new PlatformComputePortable(1);

                case TYPE_PORTABLE_JAVA:
                    return new PlatformComputeJavaPortable(1);

                case TYPE_OBJ_ARRAY:
                    return new String[] { "foo", "bar", "baz" };

                case TYPE_PORTABLE_ARRAY:
                    return new PlatformComputePortable[] {
                        new PlatformComputePortable(1),
                        new PlatformComputePortable(2),
                        new PlatformComputePortable(3)
                    };

                case TYPE_ENUM:
                    return PlatformComputeEnum.BAR;

                case TYPE_ENUM_ARRAY:
                    return new PlatformComputeEnum[] {
                        PlatformComputeEnum.BAR,
                        PlatformComputeEnum.BAZ,
                        PlatformComputeEnum.FOO
                    };

                default:
                    throw new IgniteException("Unknown type: " + type);
            }
        }
    }
}