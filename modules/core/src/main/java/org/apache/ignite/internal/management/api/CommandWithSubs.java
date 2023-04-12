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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import static org.apache.ignite.internal.management.api.BaseCommand.CMD_NAME_POSTFIX;

/**
 *
 */
public abstract class CommandWithSubs {
    /** */
    private final Map<String, Supplier<? extends Command<? extends IgniteDataTransferObject>>> commands = new LinkedHashMap<>();

    /** */
    public Collection<Supplier<? extends Command<? extends IgniteDataTransferObject>>> subcommands() {
        return commands.values();
    }

    /** */
    public void register(Supplier<? extends Command<? extends IgniteDataTransferObject>> cmd) {
        Command<?> cmdInstance = cmd.get();
        String name = cmdInstance.getClass().getSimpleName();

        if (!name.endsWith(CMD_NAME_POSTFIX))
            throw new IllegalArgumentException("Command class name must ends with 'Command'");

        commands.put(cmdInstance.getClass().getSimpleName(), cmd);
    }

    /** */
    public Command<?> command(String name) {
        return commands.get(name).get();
    }

    /** */
    public boolean positionalSubsName() {
        return true;
    }
}
