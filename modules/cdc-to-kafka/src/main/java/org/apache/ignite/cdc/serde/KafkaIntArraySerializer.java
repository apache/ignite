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

package org.apache.ignite.cdc.serde;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.kafka.common.serialization.Serializer;

/** */
public class KafkaIntArraySerializer implements Serializer<int[]> {
    /** {@inheritDoc} */
    @Override public byte[] serialize(String topic, int[] data) {
        byte[] bytes = new byte[data.length * 4];

        int off = 0;

        for (int i = 0; i < data.length; i++)
            off = U.intToBytes(data[i], bytes, off);

        return bytes;
    }
}
