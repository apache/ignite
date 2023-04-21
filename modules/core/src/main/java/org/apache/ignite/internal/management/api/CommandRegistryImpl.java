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
import org.apache.ignite.internal.management.IgniteCommandRegistry;

/**
 *
 */
public abstract class CommandRegistryImpl implements Command, CommandsRegistry {
    /** */
    private final Map<String, Command<?, ?, ?>> commands = new LinkedHashMap<>();

    /** */
    protected CommandRegistryImpl() {
        subcommands().forEach(this::register);
    }

    /** */
    void register(Command<?, ?, ?> cmd) {
        Class<? extends CommandsRegistry> parent = getClass() == IgniteCommandRegistry.class ? null : getClass();

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

    /** */
    protected abstract List<Command<?, ?, ?>> subcommands();

    /** {@inheritDoc} */
    @Override public <A extends IgniteDataTransferObject> Command<A, ?, ?> command(String name) {
        return (Command<A, ?, ?>)commands.get(name);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Map.Entry<String, Command<?, ?, ?>>> iterator() {
        return commands.entrySet().iterator();
    }

    /** {@inheritDoc} */
    @Override public String description() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Class args() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Class task() {
        throw new UnsupportedOperationException();
    }
}
