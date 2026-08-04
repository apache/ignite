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

import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * A {@link Message} that reshapes its own fields before they go on the wire and back after they arrive: copying a
 * value into the field that is actually sent, packing bits, recalculating a TTL. A message needing a
 * {@code Marshaller} for that implements {@code MarshallableMessage} instead; here there is none to use.
 *
 * @deprecated A message carries data, it is not a place to compute. Each use of this interface is a message doing
 * work that belongs to the code building or reading it, so treat the current ones as debt and add no new ones: do the
 * conversion where the message is filled in and where it is consumed.
 */
@Deprecated
public interface SelfMarshallingMessage extends Message {
    /** Called before anything else is marshalled, so a field this step fills still goes on the wire. */
    public void selfMarshal();

    /** Called after everything else is unmarshalled, so this step sees the fields already read back. */
    public void selfUnmarshal();
}
