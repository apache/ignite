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

import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * A {@link Message} that takes part in marshalling itself, so the generated companion has a step to call. What that
 * step is differs: {@link MarshallableMessage} turns fields into bytes with a {@link Marshaller},
 * {@link CustomWireFormMessage} only reshapes its own fields. Everything that treats both the same way - the check
 * against {@link org.apache.ignite.plugin.extensions.communication.NonMarshallableMessage}, requiring the generated
 * marshaller to exist - refers to this interface.
 */
public interface CustomMarshallingMessage extends Message {
    // No-op.
}
