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

package org.apache.ignite.internal.management.api;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import static org.apache.ignite.internal.management.api.Command.CMD_NAME_POSTFIX;

/**
 * All commands class names stored in registry must ends with {@link Command#CMD_NAME_POSTFIX}.
 *
 * @see org.apache.ignite.internal.management.kill.KillCommand
 * @see org.apache.ignite.internal.management.kill.KillComputeCommand
 */
public abstract class CommandRegistryImpl implements CommandsRegistry {
    /** Subcommands. */
    private final Map<String, Command<?, ?, ?>> commands = new LinkedHashMap<>();

    /** */
    protected CommandRegistryImpl() {
        subcommands().forEach(this::register);
    }

    /**
     * Register new command.
     * @param cmd Command to register.
     */
    void register(Command<?, ?, ?> cmd) {
        Class<? extends ComplexCommand<?, ?, ?>> parent = ComplexCommand.class.isAssignableFrom(getClass())
            ? (Class<? extends ComplexCommand<?, ?, ?>>)getClass()
            : null;

        String name = cmd.getClass().getSimpleName();

        if (parent != null) {
            String parentName = parent.getSimpleName();
            parentName = parentName.substring(0, parentName.length() - CMD_NAME_POSTFIX.length());

            if (!name.startsWith(parentName)) {
                throw new IllegalArgumentException(
                    "Command class name must starts with parent name [parent=" + parentName + ']');
            }

            name = name.substring(parentName.length());
        }

        if (!name.endsWith(CMD_NAME_POSTFIX))
            throw new IllegalArgumentException("Command class name must ends with 'Command'");

        name = name.substring(0, name.length() - CMD_NAME_POSTFIX.length());

        if (commands.containsKey(name))
            throw new IllegalArgumentException("Command already registered");

        commands.put(name, cmd);
    }

    /**
     * Note, this method invoked in constructor so some constant or nearly constant list is expected.
     * @return Subcommands list.
     */
    protected abstract List<Command<?, ?, ?>> subcommands();

    /** {@inheritDoc} */
    @Override public <A extends IgniteDataTransferObject> Command<A, ?, ?> command(String name) {
        return (Command<A, ?, ?>)commands.get(name);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Map.Entry<String, Command<?, ?, ?>>> commands() {
        return commands.entrySet().iterator();
    }
}
