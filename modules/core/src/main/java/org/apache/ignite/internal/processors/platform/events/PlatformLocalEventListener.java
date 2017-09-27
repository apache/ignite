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

package org.apache.ignite.internal.processors.platform.events;

import org.apache.ignite.Ignite;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Platform local event filter. Delegates apply to native platform.
 */
public class PlatformLocalEventListener implements IgnitePredicate<Event> {
    /** */
    private static final long serialVersionUID = 0L;
    
    /** Listener id. */
    private final int id;

    /** Ignite. */
    @SuppressWarnings("unused")
    @IgniteInstanceResource
    private transient Ignite ignite;

    /**
     * Constructor.
     *
     * @param id Listener id.
     */
    public PlatformLocalEventListener(int id) {
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(Event evt) {
        assert ignite != null;

        PlatformContext ctx = PlatformUtils.platformContext(ignite);

        assert ctx != null;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx writer = ctx.writer(out);

            writer.writeInt(id);

            ctx.writeEvent(writer, evt);

            out.synchronize();

            long res = ctx.gateway().eventLocalListenerApply(mem.pointer());

            return res != 0;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return this == o || o != null && getClass() == o.getClass() && id == ((PlatformLocalEventListener) o).id;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id;
    }
}
