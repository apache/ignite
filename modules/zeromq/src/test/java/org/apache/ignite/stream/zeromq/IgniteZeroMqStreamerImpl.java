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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.apache.ignite.stream.zeromq.converter.DefaultMessageConverter;
import org.apache.ignite.stream.zeromq.converter.MessageConverter;

public class IgniteZeroMqStreamerImpl extends IgniteZeroMqStreamer<Long, String> {
    /**
     * @param zeroMqSettings ZeroMQ settings.
     */
    public IgniteZeroMqStreamerImpl(ZeroMqSettings zeroMqSettings) {
        super(zeroMqSettings);

        setSingleTupleExtractor(new ZeroMqStreamSingleTupleExtractorImpl());
    }

    class ZeroMqStreamSingleTupleExtractorImpl implements StreamSingleTupleExtractor<byte[], Long, String> {

        @Override public Map.Entry<Long, String> extract(byte[] msg) {
            try {
                MessageConverter message = new DefaultMessageConverter();
                message.convert(msg);

                return new IgniteBiTuple<>(message.getId(), message.getMessage());
            }
            catch (IgniteException e) {
                U.error(log, e);

                return null;
            }
        }
    }
}
