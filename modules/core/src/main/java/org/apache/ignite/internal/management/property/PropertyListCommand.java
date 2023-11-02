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

package org.apache.ignite.internal.management.property;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.management.SystemViewCommand;
import org.apache.ignite.internal.management.SystemViewTask;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class PropertyListCommand implements ComputeCommand<PropertyListCommandArg, PropertiesListResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Print list of available properties";
    }

    /** {@inheritDoc} */
    @Override public Class<PropertyListCommandArg> argClass() {
        return PropertyListCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<PropertiesListTask> taskClass() {
        return PropertiesListTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(PropertyListCommandArg arg, PropertiesListResult res, Consumer<String> printer) {
        if (arg.printValues()) {
            List<SystemViewTask.SimpleType> types = res.titles().stream()
                .map(x -> SystemViewTask.SimpleType.STRING).collect(Collectors.toList());

            List<List<?>> data = res.properties().keySet().stream()
                .map(key -> F.asList(F.asList(key),
                    F.asList(res.properties().get(key).get(0)),
                    F.asList(res.properties().get(key).get(1))))
                .collect(Collectors.toList());

            SystemViewCommand.printTable(res.titles(), types, data, printer);
        }
        else {
            for (String prop : res.properties().keySet())
                printer.accept(prop);
        }
    }
}
