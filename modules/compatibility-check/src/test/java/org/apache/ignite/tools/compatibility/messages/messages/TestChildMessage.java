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

package org.apache.ignite.tools.compatibility.messages.messages;

import java.util.List;
import org.apache.ignite.internal.Compress;
import org.apache.ignite.internal.CustomMapper;
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.NioField;
import org.apache.ignite.internal.Order;

/** Declaration order is deliberately different from serialization order. */
public class TestChildMessage extends TestParentMessage {
    /** Enum mapped by a named mapper. */
    @Order(1)
    @CustomMapper("example.Mapper")
    private Mode mode;

    /** Child field hides the parent's field. */
    @Order(0)
    @Compress
    @NioField
    private byte[] id;

    /** Logical value mapped to a wire field. */
    @Marshalled("id")
    private List<? extends String> payload;

    /** Ordinary, non-wire field. */
    private int ignored;

    /** Ordinary methods do not become schema entries. */
    int ignored() {
        return ignored;
    }

    /** Fixture enum. */
    public enum Mode {
        /** Single fixture value. */
        ONE
    }
}
