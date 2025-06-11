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

package org.apache.ignite.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandsProvider;
import org.apache.ignite.internal.util.typedef.F;

/** Enables registration of additional commands for testing purposes. */
public class TestCommandsProvider implements CommandsProvider {
    /** Commands. */
    private static final Set<Command<?, ?>> COMMANDS = ConcurrentHashMap.newKeySet();

    /**
     * Registers new commands.
     *
     * @param cmds Commands to register.
     */
    public static void registerCommands(Command<?, ?>... cmds) {
        COMMANDS.addAll(F.asList(cmds));
    }

    /** Unregisters all registered commands. */
    public static void unregisterAll() {
        COMMANDS.clear();
    }

    /** {@inheritDoc} */
    @Override public Collection<Command<?, ?>> commands() {
        return Collections.unmodifiableSet(COMMANDS);
    }
}
