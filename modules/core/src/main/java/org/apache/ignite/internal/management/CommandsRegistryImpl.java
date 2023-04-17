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

package org.apache.ignite.internal.management;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandWithSubs;
import org.apache.ignite.internal.management.kill.KillCommand;

/**
 *
 */
public class CommandsRegistryImpl extends CommandWithSubs {
    /** TODO: move instance to command-handler. Each node will use own instance of registry. */
    public static final CommandsRegistryImpl INSTANCE = new CommandsRegistryImpl();

    static {
        INSTANCE.registerAll();
    }

    /** {@inheritDoc} */
    @Override protected List<Command<?, ?, ?>> subcommands() {
        return Arrays.<Command<?, ?, ?>>asList(
            new SystemViewCommand(),
            new KillCommand()
        );
    }

    /** {@inheritDoc} */
    @Override public void registerAll() {
        super.registerAll();
    }
}
