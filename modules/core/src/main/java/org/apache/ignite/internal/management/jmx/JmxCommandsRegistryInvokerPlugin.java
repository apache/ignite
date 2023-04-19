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
import java.util.Map;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandWithSubs;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.jetbrains.annotations.Nullable;
import static org.apache.ignite.internal.util.IgniteUtils.JMX_DOMAIN;
import static org.apache.ignite.internal.util.IgniteUtils.appendClassLoaderHash;
import static org.apache.ignite.internal.util.IgniteUtils.appendJvmId;

/**
 *
 */
public class JmxCommandsRegistryInvokerPlugin implements IgnitePlugin {
    /** */
    private PluginContext ctx;

    /** */
    private IgniteLogger log;

    /** */
    private IgniteEx grid;

    /** */
    private final List<ObjectName> mBeans = new ArrayList<>();

    /** */
    public void context(PluginContext ctx) {
        this.ctx = ctx;
        grid = (IgniteEx)ctx.grid();
        log = ctx.log(JmxCommandsRegistryInvokerPlugin.class);
    }

    /** */
    public void onIgniteStart() {
        if (U.IGNITE_MBEANS_DISABLED) {
            log.info("Plugin disabled, IGNITE_MBEANS_DISABLED = true.");

            return;
        }

        grid.commands().forEach(cmd -> register(cmd.getKey(), new LinkedList<>(), cmd.getValue()));
    }

    /** */
    public <A extends IgniteDataTransferObject> void register(String name, List<String> parents, Command<A, ?, ?> cmd) {
        if (cmd instanceof CommandsRegistry) {
            parents.add(name);

            ((Iterable<Map.Entry<String, Command<?, ?, ?>>>)cmd).forEach(
                cmd0 -> register(cmd0.getKey(), parents, cmd0.getValue()
            ));

            parents.remove(parents.size() - 1);

            if (!((CommandWithSubs)cmd).canBeExecuted())
                return;
        }

        try {
            ObjectName mbean = U.registerMBean(
                grid.configuration().getMBeanServer(),
                makeMBeanName(grid.context().igniteInstanceName(), parents, name),
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

    /** TODO: move to the rest JXM name methods. */
    public static ObjectName makeMBeanName(@Nullable String igniteInstanceName, List<String> parents, String name)
        throws MalformedObjectNameException {
        SB sb = new SB(JMX_DOMAIN + ':');

        appendClassLoaderHash(sb);

        appendJvmId(sb);

        if (igniteInstanceName != null && !igniteInstanceName.isEmpty())
            sb.a("igniteInstanceName=").a(igniteInstanceName).a(',');

        sb.a("group=").a("management").a(',');

        for (int i = 0; i < parents.size(); i++)
            sb.a("parent").a(i).a("=").a(parents.get(i)).a(',');

        sb.a("name=").a(name);

        return new ObjectName(sb.toString());
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
