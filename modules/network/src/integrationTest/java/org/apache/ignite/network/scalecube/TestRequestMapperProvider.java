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

package org.apache.ignite.network.scalecube;

import java.io.IOException;
import org.apache.ignite.network.message.MessageDeserializer;
import org.apache.ignite.network.message.MessageMapperProvider;
import org.apache.ignite.network.message.MessageMappingException;
import org.apache.ignite.network.message.MessageSerializer;

/**
 * Mapper for {@link TestRequest}.
 */
public class TestRequestMapperProvider implements MessageMapperProvider<TestRequest> {
    /** {@inheritDoc} */
    @Override public MessageDeserializer<TestRequest> createDeserializer() {
        return reader -> {
            try {
                final int number = reader.stream().readInt();
                return new TestRequest(number);
            }
            catch (IOException e) {
                throw new MessageMappingException("Failed to deserialize", e);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public MessageSerializer<TestRequest> createSerializer() {
        return (message, writer) -> {
            try {
                writer.stream().writeInt(message.number());
            }
            catch (IOException e) {
                throw new MessageMappingException("Failed to serialize", e);
            }
        };
    }
}
