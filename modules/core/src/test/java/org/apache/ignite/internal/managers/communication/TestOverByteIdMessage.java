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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Test message with over byte direct type. */
public class TestOverByteIdMessage implements Message {
    /** Direct type. */
    static final short DIRECT_TYPE_OVER_BYTE = 1000;

    /** */
    @Order(0)
    int val;

    /** {@inheritDoc} */
    @Override public short directType() {
        return DIRECT_TYPE_OVER_BYTE;
    }
}
