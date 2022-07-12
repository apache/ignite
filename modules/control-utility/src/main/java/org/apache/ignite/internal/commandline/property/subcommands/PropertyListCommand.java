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

package org.apache.ignite.internal.commandline.property.subcommands;

import java.util.logging.Logger;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.meta.subcommands.VoidDto;
import org.apache.ignite.internal.commandline.property.PropertySubCommandsList;
import org.apache.ignite.internal.commandline.property.tasks.PropertiesListResult;
import org.apache.ignite.internal.commandline.property.tasks.PropertiesListTask;

/** */
public class PropertyListCommand extends PropertyAbstractSubCommand<VoidDto, PropertiesListResult> {
    /** {@inheritDoc} */
    @Override protected String taskName() {
        return PropertiesListTask.class.getName();
    }

    /** {@inheritDoc} */
    @Override public VoidDto parseArguments0(CommandArgIterator argIter) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void printResult(PropertiesListResult res, Logger log) {
        for (String prop : res.properties())
            log.info(prop);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return PropertySubCommandsList.LIST.text();
    }
}
