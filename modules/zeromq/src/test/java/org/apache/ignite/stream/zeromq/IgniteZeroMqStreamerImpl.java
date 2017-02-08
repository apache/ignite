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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.apache.ignite.stream.zeromq.converter.DefaultMessageConverter;

/**
 * Implementation ZeroMQ streamer
 */
public class IgniteZeroMqStreamerImpl extends IgniteZeroMqStreamer<Integer, String> {
    /** Counter. */
    private AtomicInteger count = new AtomicInteger();

    /**
     * @param zeroMqSettings ZeroMQ settings.
     */
    public IgniteZeroMqStreamerImpl(ZeroMqSettings zeroMqSettings) {
        super(zeroMqSettings);

        count.set(0);

        setSingleTupleExtractor(new ZeroMqStreamSingleTupleExtractorImpl());
    }

    /**
     * Implementation single tuple extractor for ZeroMQ streamer.
     */
    class ZeroMqStreamSingleTupleExtractorImpl implements StreamSingleTupleExtractor<byte[], Integer, String> {

        @Override public Map.Entry<Integer, String> extract(byte[] msg) {
            try {
                DefaultMessageConverter message = new DefaultMessageConverter();
                message.convert(msg);

                return new IgniteBiTuple<>(count.getAndIncrement(), message.getValue());
            }
            catch (IgniteException e) {
                U.error(log, e);

                return null;
            }
        }
    }
}
