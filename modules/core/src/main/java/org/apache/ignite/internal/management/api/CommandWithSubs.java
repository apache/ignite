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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import static org.apache.ignite.internal.management.api.BaseCommand.CMD_NAME_POSTFIX;

/**
 *
 */
public abstract class CommandWithSubs implements Command {
    /** */
    private final Map<String, Supplier<? extends Command<?, ?, ?>>> commands = new LinkedHashMap<>();

    /** */
    public Collection<Supplier<? extends Command<?, ?, ?>>> subcommands() {
        return commands.values();
    }

    /** */
    public void register(Supplier<? extends Command<?, ?, ?>> cmd) {
        Command<?, ?, ?> cmdInstance = cmd.get();

        String name = cmdInstance.getClass().getSimpleName();

        if (!name.endsWith(CMD_NAME_POSTFIX))
            throw new IllegalArgumentException("Command class name must ends with 'Command'");

        name = name.substring(0, name.length() - CMD_NAME_POSTFIX.length());

        if (commands.containsKey(name))
            throw new IllegalArgumentException("Command already registered");

        commands.put(name, cmd);
    }

    /** */
    public Command<?, ?, ?> command(String name) {
        Supplier<? extends Command<?, ?, ?>> supplier = commands.get(name);

        if (supplier == null)
            return null;

        return supplier.get();
    }

    /** */
    public boolean canBeExecuted() {
        return true;
    }
}
