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

package org.apache.ignite.stream.zeromq;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.stream.StreamSingleTupleExtractor;

/**
 * Default implementation single tuple extractor for ZeroMQ streamer.
 */
public class ZeroMqStringSingleTupleExtractor implements StreamSingleTupleExtractor<byte[], Integer, String> {
    /** Counter. */
    private AtomicInteger count = new AtomicInteger();

    public ZeroMqStringSingleTupleExtractor() {
        count.set(0);
    }

    @Override public Map.Entry<Integer, String> extract(byte[] msg) {
        try {
            return new IgniteBiTuple<>(count.getAndIncrement(), new String(msg, Charset.forName("UTF-8")));
        }
        catch (IgniteException e) {
            return null;
        }
    }
}
