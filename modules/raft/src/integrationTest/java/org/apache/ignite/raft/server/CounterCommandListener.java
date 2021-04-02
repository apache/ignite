/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.server;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;

/** */
public class CounterCommandListener implements RaftGroupCommandListener {
    /** */
    private AtomicInteger counter = new AtomicInteger();

    /** {@inheritDoc} */
    @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<ReadCommand> clo = iterator.next();

            assert clo.command() instanceof GetValueCommand;

            clo.success(counter.get());
        }
    }

    /** {@inheritDoc} */
    @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<WriteCommand> clo = iterator.next();

            IncrementAndGetCommand cmd0 = (IncrementAndGetCommand) clo.command();

            clo.success(counter.addAndGet(cmd0.delta()));
        }
    }
}
