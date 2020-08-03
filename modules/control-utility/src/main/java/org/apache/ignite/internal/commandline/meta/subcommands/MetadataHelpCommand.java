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

package org.apache.ignite.internal.commandline.meta.subcommands;

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.meta.MetadataCommand;
import org.apache.ignite.internal.commandline.meta.MetadataSubCommandsList;

/** */
public class MetadataHelpCommand implements Command<Void> {
    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        throw new UnsupportedOperationException("printUsage");
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        new MetadataCommand().printUsage(log);

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean experimentalEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return MetadataSubCommandsList.HELP.text();
    }
}
