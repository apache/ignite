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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.ReflectionException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.PositionalArgument;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import static java.util.Collections.singleton;
import static javax.management.MBeanOperationInfo.ACTION;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;

/**
 *
 */
public class CommandMBean<A extends IgniteDataTransferObject> implements DynamicMBean {
    /** */
    public static final String METHOD = "invoke";

    /** */
    private final IgniteEx grid;

    /** */
    private final Command<A, ?, ?> cmd;

    /** */
    public CommandMBean(IgniteEx grid, Command<A, ?, ?> cmd) {
        this.grid = grid;
        this.cmd = cmd;
    }

    /** {@inheritDoc} */
    @Override public Object getAttribute(
        String attribute
    ) throws AttributeNotFoundException, MBeanException, ReflectionException {
        throw new UnsupportedOperationException("getAttribute");
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(
        Attribute attribute
    ) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new UnsupportedOperationException("setAttribute");
    }

    /** {@inheritDoc} */
    @Override public AttributeList getAttributes(String[] attributes) {
        throw new UnsupportedOperationException("getAttributes");
    }

    /** {@inheritDoc} */
    @Override public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException("setAttributes");
    }

    /** {@inheritDoc} */
    @Override public Object invoke(
        String actionName,
        Object[] params,
        String[] signature
    ) throws MBeanException, ReflectionException {
        if (!METHOD.equals(actionName))
            throw new UnsupportedOperationException(actionName);

        Map<Field, String> vals = new HashMap<>();
        AtomicInteger cntr = new AtomicInteger();

        Consumer<Field> fillVals = fld -> {
            String val = (String)params[cntr.getAndIncrement()];

            if (val.isEmpty())
                return;

            vals.put(fld, val);
        };

        visitCommandParams(cmd.args(), fillVals, fillVals, (optional, flds) -> flds.forEach(fillVals));

        Function<Field, Object> val = fld -> Optional
            .ofNullable(vals.get(fld))
            .map(v -> CommandUtils.parseVal(v, fld.getType()))
            .orElse(null);

        try {
            A arg = CommandUtils.arguments(cmd.args(), (fld, pos) -> val.apply(fld), val);

            IgniteCompute compute = grid.compute();

            Map<UUID, ClusterNode> clusterNodes = grid.cluster().nodes().stream()
                .collect(Collectors.toMap(ClusterNode::id, n -> n));

            Collection<UUID> nodeIds = cmd.nodes(clusterNodes.keySet(), arg);

            for (UUID id : nodeIds) {
                if (!clusterNodes.containsKey(id))
                    throw new IllegalArgumentException("Node with id=" + id + " not found.");
            }

            if (nodeIds.isEmpty())
                nodeIds = singleton(grid.localNode().id());

            if (!F.isEmpty(nodeIds))
                compute = grid.compute(grid.cluster().forNodeIds(nodeIds));

            Object res = compute.execute(cmd.task().getName(), new VisorTaskArgument<>(nodeIds, arg, false));

            StringBuilder resStr = new StringBuilder();

            cmd.printResult(arg, res, str -> resStr.append(str).append('\n'));

            return resStr.toString();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MBeanInfo getMBeanInfo() {
        return new MBeanInfo(
            CommandMBean.class.getName(),
            cmd.getClass().getSimpleName(),
            null,
            null,
            new MBeanOperationInfo[]{
                new MBeanOperationInfo(METHOD, cmd.description(), arguments(), String.class.getName(), ACTION)
            },
            null
        );
    }

    /** */
    public MBeanParameterInfo[] arguments() {
        List<MBeanParameterInfo> args = new ArrayList<>();

        Consumer<Field> fldCnsmr = fld -> {
            String descStr;

            if (!fld.isAnnotationPresent(EnumDescription.class)) {
                Argument desc = fld.getAnnotation(Argument.class);
                PositionalArgument posDesc = fld.getAnnotation(PositionalArgument.class);

                descStr = desc != null ? desc.description() : posDesc.description();
            }
            else {
                EnumDescription enumDesc = fld.getAnnotation(EnumDescription.class);

                String[] names = enumDesc.names();
                String[] descriptions = enumDesc.descriptions();

                StringBuilder bldr = new StringBuilder();

                for (int i = 0; i < names.length; i++) {
                    if (i != 0)
                        bldr.append('\n');
                    bldr.append(names[i]).append(" - ").append(descriptions[i]);
                }

                descStr = bldr.toString();
            }

            args.add(new MBeanParameterInfo(fld.getName(), String.class.getName(), descStr));
        };

        visitCommandParams(cmd.args(), fldCnsmr, fldCnsmr, (optional, flds) -> flds.forEach(fldCnsmr));

        return args.toArray(new MBeanParameterInfo[args.size()]);
    }
}
