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

package org.apache.ignite.internal.commands.persistence;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.internal.commands.api.Command;
import org.apache.ignite.internal.commands.api.CommandWithSubs;

/**
 *
 */
public class PersistenceCommand implements CommandWithSubs {
    /** {@inheritDoc} */
    @Override public Collection<Command> subcommands() {
        return Arrays.asList(
            new PersistenceInfoCommand(),
            new PersistenceCleanCommand(),
            new PersistenceBackupCommand()
        );
    }

    /** {@inheritDoc} */
    @Override public boolean canBeExecuted() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return "Print information about potentially corrupted caches on local node";
    }
}
