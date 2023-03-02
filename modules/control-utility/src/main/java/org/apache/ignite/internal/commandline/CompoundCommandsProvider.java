/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import org.apache.ignite.internal.util.typedef.F;

/**
 * Compound control utility commands provider.
 */
public class CompoundCommandsProvider implements CommandsProvider {
    /** */
    private final Iterable<CommandsProvider> providers;

    /** */
    public CompoundCommandsProvider(Iterable<CommandsProvider> providers) {
        this.providers = providers;
    }

    /** {@inheritDoc} */
    @Override public Command<?> parse(String str) {
        Command<?> cmd = null;

        for (CommandsProvider provider : providers) {
            if (cmd == null)
                cmd = provider.parse(str);
            else if (provider.parse(str) != null) {
                throw new IllegalArgumentException("Only one action can be specified, but found at least two:" +
                    cmd.toString() + ", " + provider.parse(str).toString());
            }
        }

        return cmd;
    }

    /** {@inheritDoc} */
    @Override public Iterable<Command<?>> commands() {
        return F.flat(F.iterator(providers, CommandsProvider::commands, true));
    }
}
