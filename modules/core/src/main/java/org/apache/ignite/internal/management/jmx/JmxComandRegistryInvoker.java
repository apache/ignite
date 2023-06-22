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

package org.apache.ignite.internal.management.jmx;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;

import static org.apache.ignite.internal.management.api.CommandUtils.executable;
import static org.apache.ignite.internal.util.IgniteUtils.makeMBeanName;

/** */
public class JmxComandRegistryInvoker implements IgnitePlugin {
    /** */
    private IgniteLogger log;

    /** */
    private IgniteEx grid;

    /** */
    private final List<ObjectName> mBeans = new ArrayList<>();

    /** */
    public void context(PluginContext ctx) {
        this.grid = (IgniteEx)ctx.grid();
        this.log = ctx.log(JmxComandRegistryInvoker.class);
    }

    /** */
    public void onIgniteStart() {
        if (U.IGNITE_MBEANS_DISABLED) {
            log.info("Plugin disabled, IGNITE_MBEANS_DISABLED = true.");

            return;
        }

        grid.commands().commands().forEachRemaining(cmd -> register(cmd.getKey(), new LinkedList<>(), cmd.getValue()));
    }

    /** */
    public void register(String name, List<String> parents, Command<?, ?> cmd) {
        if (cmd instanceof CommandsRegistry) {
            parents.add(name);

            ((CommandsRegistry<?, ?>)cmd).commands()
                .forEachRemaining(cmd0 -> register(cmd0.getKey(), parents, cmd0.getValue()));

            parents.remove(parents.size() - 1);

            if (!executable(cmd))
                return;
        }

        try {
            ObjectName mbean = U.registerMBean(
                grid.configuration().getMBeanServer(),
                makeMBeanName(grid.context().igniteInstanceName(), "management", parents, name),
                new CommandMBean<>(grid, cmd),
                CommandMBean.class
            );

            mBeans.add(mbean);

            if (log.isDebugEnabled())
                log.debug("Command JMX bean created. " + mbean);
        }
        catch (JMException e) {
            log.error("MBean for command '" + name + "' can't be created.", e);
        }
    }

    /** */
    public void onIgniteStop() {
        MBeanServer jmx = grid.configuration().getMBeanServer();

        for (ObjectName mBean : mBeans) {
            try {
                jmx.unregisterMBean(mBean);

                if (log.isDebugEnabled())
                    log.debug("Unregistered command MBean: " + mBean);
            }
            catch (JMException e) {
                log.error("Failed to unregister command MBean: " + mBean, e);
            }
        }
    }
}
