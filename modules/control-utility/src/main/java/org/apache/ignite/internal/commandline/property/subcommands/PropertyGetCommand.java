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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.property.PropertyArgs;
import org.apache.ignite.internal.commandline.property.PropertySubCommandsList;
import org.apache.ignite.internal.commandline.property.tasks.PropertyOperationResult;
import org.apache.ignite.internal.commandline.property.tasks.PropertyTask;

/** */
public class PropertyGetCommand extends PropertyAbstractSubCommand<PropertyArgs, PropertyOperationResult> {
    /** {@inheritDoc} */
    @Override protected String taskName() {
        return PropertyTask.class.getName();
    }

    /** {@inheritDoc} */
    @Override public PropertyArgs parseArguments0(CommandArgIterator argIter) {
        String name = null;

        while (argIter.hasNextSubArg() && name == null) {
            String optName = argIter.nextArg("Expecting " + PropertyArgs.NAME);

            if (PropertyArgs.NAME.equals(optName))
                name = argIter.nextArg("property name");
        }

        if (name == null) {
            throw new IllegalArgumentException("Property name is not specified. " +
                "Please use the option: --name <property_name>");
        }

        return new PropertyArgs(name, null, PropertyArgs.Action.GET);
    }

    /** {@inheritDoc} */
    @Override protected void printResult(PropertyOperationResult res, IgniteLogger log) {
        log.info(arg().name() + " = " + res.value());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return PropertySubCommandsList.GET.text();
    }
}
