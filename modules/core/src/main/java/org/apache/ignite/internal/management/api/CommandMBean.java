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

package org.apache.ignite.internal.management.api;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.F;

import static javax.management.MBeanOperationInfo.ACTION;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;

/**
 * Bean expose single mamagement command via JMX interface.
 *
 * @see Command
 * @see CommandsRegistry
 */
public class CommandMBean<A extends IgniteDataTransferObject, R> implements DynamicMBean {
    /** Name of the JMX method to invoke command. */
    public static final String INVOKE = "invoke";

    /**
     * Used for tests.
     * Name of the method to retrive last method result.
     */
    public static final String LAST_RES_METHOD = "lastResult";

    /** Local ignite node. */
    private final IgniteEx ignite;

    /** Logger. */
    private final IgniteLogger log;

    /** Management command to expose. */
    private final Command<A, ?> cmd;

    /**
     * Used for tests.
     * Last invocation result.
     */
    private R res;

    /**
     * @param ignite Ignite node.
     * @param cmd Management command to expose.
     */
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

            CommandInvoker<A> invoker = new CommandInvoker<>(cmd, new ParamsToArgument(params).argument(), ignite);

            if (invoker.prepare(printer))
                res = invoker.invoke(printer, false);

            return resStr.toString();
        }
        catch (Exception e) {
            log.error("Invoke error:", e);

            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MBeanInfo getMBeanInfo() {
        List<MBeanParameterInfo> args = new ArrayList<>();

        BiConsumer<ArgumentGroup, Field> fldCnsmr = (argGrp, fld) -> {
            String descStr = "";

            if ((argGrp != null && argGrp.optional()) || fld.getAnnotation(Argument.class).optional())
                descStr += "Optional. ";

            if (!fld.isAnnotationPresent(EnumDescription.class))
                descStr += fld.getAnnotation(Argument.class).description();
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

                descStr += bldr.toString();
            }

            args.add(new MBeanParameterInfo(fld.getName(), String.class.getName(), descStr));
        };

        visitCommandParams(
            cmd.argClass(),
            fld -> fldCnsmr.accept(null, fld),
            fld -> fldCnsmr.accept(null, fld),
            (grp, flds) -> flds.forEach(fld -> fldCnsmr.accept(grp, fld))
        );

        return new MBeanInfo(
            CommandMBean.class.getName(),
            cmd.getClass().getSimpleName(),
            null,
            null,
            new MBeanOperationInfo[]{
                new MBeanOperationInfo(
                    INVOKE,
                    cmd.description(),
                    args.toArray(new MBeanParameterInfo[0]),
                    String.class.getName(),
                    ACTION
                )
            },
            null
        );
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
