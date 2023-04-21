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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.AbstractCommandInvoker;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.EnumDescription;
import static javax.management.MBeanOperationInfo.ACTION;

/**
 * MBean to expose single command.
 */
public class CommandMBean<A extends IgniteDataTransferObject> extends AbstractCommandInvoker implements DynamicMBean {
    /** Each command exposed via JMX has name "invoke". */
    public static final String METHOD = "invoke";

    /** Local node. */
    private final IgniteEx grid;

    /** Command to expose. */
    private final Command<A, ?, ?> cmd;

    /**
     * @param grid Local node.
     * @param cmd Command to expose.
     */
    public CommandMBean(IgniteEx grid, Command<A, ?, ?> cmd) {
        this.grid = grid;
        this.cmd = cmd;
    }

    /** {@inheritDoc} */
    @Override public Object invoke(
        String actionName,
        Object[] params,
        String[] signature
    ) throws MBeanException, ReflectionException {
        if (!METHOD.equals(actionName))
            throw new UnsupportedOperationException(actionName);

        Map<String, String> vals = new HashMap<>();
        AtomicInteger cntr = new AtomicInteger();

        Consumer<Field> fillVals = fld -> {
            String val = (String)params[cntr.getAndIncrement()];

            if (val.isEmpty())
                return;

            vals.put(fld.getName(), val);
        };

        visitCommandParams(cmd.args(), fillVals, fillVals, (optional, flds) -> flds.forEach(fillVals));

        StringBuilder resStr = new StringBuilder();

        execute(
            cmd,
            vals,
            line -> resStr.append(line).append('\n')
        );

        return resStr.toString();
    }

    /** {@inheritDoc} */
    @Override public MBeanInfo getMBeanInfo() {
        List<MBeanParameterInfo> args = new ArrayList<>();

        Consumer<Field> fldCnsmr = fld -> {
            String descStr;

            if (!fld.isAnnotationPresent(EnumDescription.class))
                descStr = fld.getAnnotation(Argument.class).description();
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

        return new MBeanInfo(
            CommandMBean.class.getName(),
            cmd.getClass().getSimpleName(),
            null,
            null,
            new MBeanOperationInfo[]{
                new MBeanOperationInfo(
                    METHOD,
                    cmd.description(),
                    args.toArray(new MBeanParameterInfo[args.size()]),
                    String.class.getName(),
                    ACTION
                )
            },
            null
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteEx grid() {
        return grid;
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
}
