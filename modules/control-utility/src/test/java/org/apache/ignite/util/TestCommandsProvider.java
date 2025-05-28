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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandsProvider;

/** Enables registration of additional commands for testing purposes. */
public class TestCommandsProvider implements CommandsProvider {
    /** Commands. */
    private static final Set<Class<? extends Command<?, ?>>> COMMANDS = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * Registers a new command.
     *
     * @param cmdCls Command class.
     */
    public static void registerCommand(Class<? extends Command<?, ?>> cmdCls) {
        COMMANDS.add(cmdCls);
    }

    /** Unregisters all registered commands. */
    public static void unregisterAll() {
        COMMANDS.clear();
    }

    /** {@inheritDoc} */
    @Override public Collection<Command<?, ?>> commands() {
        try {
            Set<Command<?, ?>> cmds = new HashSet<>();

            for (Class<? extends Command<?, ?>> clazz : COMMANDS)
                cmds.add(clazz.getDeclaredConstructor().newInstance());

            return cmds;
        }
        catch (Exception e) {
            throw new IgniteException("Failed to register test commands", e);
        }
    }
}
