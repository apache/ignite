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

package org.apache.ignite.internal.managers.management;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.NodeCommandInvoker;
import org.apache.ignite.internal.util.typedef.F;

import static javax.management.MBeanOperationInfo.ACTION;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;

/** */
public class CommandMBean<A extends IgniteDataTransferObject, R> implements DynamicMBean {
    /** */
    public static final String INVOKE = "invoke";

    /** */
    public static final String LAST_RES_METHOD = "lastResult";

    /** */
    private final IgniteEx ignite;

    /** */
    private final IgniteLogger log;

    /** */
    private final Command<A, ?> cmd;

    /** */
    private R res;

    /** */
    public CommandMBean(IgniteEx ignite, Command<A, R> cmd) {
        this.ignite = ignite;
        this.cmd = cmd;
        this.log = ignite.log().getLogger(CommandMBean.class.getName() + '#' + cmd.getClass().getSimpleName());
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
    @Override public Object invoke(
        String actionName,
        Object[] params,
        String[] signature
    ) throws MBeanException, ReflectionException {
        // Default JMX invoker pass arguments in for params: Object[] = { "invoke", parameter_values_array, types_array}
        // while JConsole pass params values directly in params array.
        // This check supports both way of invocation.
        if (params.length == 3
            && (params[0].equals(INVOKE) || params[0].equals(LAST_RES_METHOD))
            && params[1] instanceof Object[])
            return invoke((String)params[0], (Object[])params[1], (String[])params[2]);

        if (LAST_RES_METHOD.equals(actionName))
            return res;

        if (!INVOKE.equals(actionName))
            throw new UnsupportedOperationException(actionName);

        try {
            StringBuilder resStr = new StringBuilder();

            Consumer<String> printer = str -> resStr.append(str).append('\n');

            NodeCommandInvoker<A> invoker = new NodeCommandInvoker<>(cmd, new ParamsToArgument(params).argument(), ignite);

            if (invoker.prepare(printer))
                res = invoker.invoke(printer, false);

            return resStr.toString();
        }
        catch (GridClientException e) {
            log.error("Invoke error:", e);

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
                new MBeanOperationInfo(INVOKE, cmd.description(), argumentsDescription(), String.class.getName(), ACTION)
            },
            null
        );
    }

    /** */
    public MBeanParameterInfo[] argumentsDescription() {
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

        visitCommandParams(cmd.argClass(), fldCnsmr, fldCnsmr, (optional, flds) -> flds.forEach(fldCnsmr));

        return args.toArray(new MBeanParameterInfo[args.size()]);
    }

    /** {@inheritDoc} */
    @Override public AttributeList getAttributes(String[] attributes) {
        throw new UnsupportedOperationException("getAttributes");
    }

    /** {@inheritDoc} */
    @Override public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException("setAttributes");
    }

    /** */
    private class ParamsToArgument implements Function<Field, Object> {
        /** */
        private int cntr;

        /** */
        private final Object[] vals;

        /** */
        private ParamsToArgument(Object[] vals) {
            this.vals = vals;
        }

        /** */
        public A argument() {
            // This will map vals to argument fields.
            return CommandUtils.argument(cmd.argClass(), (fld, pos) -> apply(fld), this);
        }

        /** {@inheritDoc} */
        @Override public Object apply(Field field) {
            String val = (String)vals[cntr];

            cntr++;

            return !F.isEmpty(val) ? CommandUtils.parseVal(val, field.getType()) : null;
        }
    }
}
