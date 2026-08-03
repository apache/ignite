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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * A {@link Message} that converts its own fields to and from the form that goes on the wire: copying a value into the
 * field that is actually sent, packing bits, recalculating a TTL. Unlike {@link MarshallableMessage} it needs no
 * {@link Marshaller} to do that.
 */
public interface CustomWireFormMessage extends Message {
    /** Converts the fields into the form that goes on the wire. Called before sending. */
    public void toWireForm() throws IgniteCheckedException;

    /** Converts the fields back from the form they arrived in. Called after receiving. */
    public void fromWireForm() throws IgniteCheckedException;
}
