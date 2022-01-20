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

package org.apache.ignite.internal.network.serialization.marshal;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * (Un)marshalling logic specific to {@link Proxy} instances.
 */
class ProxyMarshaller {
    private final ValueWriter<Object> valueWriter;
    private final ValueReader<Object> valueReader;

    /**
     * The placeholder to use when pre-instantiating a Proxy (because we didn't read the actual handler yet).
     */
    private static final InvocationHandler placeholderInvocationHandler = new PlaceholderInvocationHandler();

    private static final Field proxyHandlerField = findProxyHandlerField();

    private static Field findProxyHandlerField() {
        Field field;
        try {
            field = Proxy.class.getDeclaredField("h");
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }

        field.setAccessible(true);

        return field;
    }

    ProxyMarshaller(ValueWriter<Object> valueWriter, ValueReader<Object> valueReader) {
        this.valueWriter = valueWriter;
        this.valueReader = valueReader;
    }

    boolean isProxyClass(Class<?> classToCheck) {
        return Proxy.isProxyClass(classToCheck);
    }

    void writeProxy(Object proxy, DataOutputStream output, MarshallingContext context) throws MarshalException, IOException {
        assert Proxy.isProxyClass(proxy.getClass());

        BuiltInMarshalling.writeClassArray(proxy.getClass().getInterfaces(), output, context);

        valueWriter.write(Proxy.getInvocationHandler(proxy), output, context);
    }

    Object preInstantiateProxy(DataInputStream input, UnmarshallingContext context) throws UnmarshalException, IOException {
        Class<?>[] interfaces = BuiltInMarshalling.readClassArray(input, context);

        return Proxy.newProxyInstance(context.classLoader(), interfaces, placeholderInvocationHandler);
    }

    void fillProxyFrom(DataInputStream input, Object proxyToFill, UnmarshallingContext context) throws UnmarshalException, IOException {
        InvocationHandler invocationHandler = readInvocationHandler(input, context);
        replaceInvocationHandler(proxyToFill, invocationHandler);
    }

    private InvocationHandler readInvocationHandler(DataInputStream input, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        Object object = valueReader.read(input, context);
        if (!(object instanceof InvocationHandler)) {
            throw new UnmarshalException("Expected an InvocationHandler, but read " + object);
        }
        return (InvocationHandler) object;
    }

    private void replaceInvocationHandler(Object objectToFill, InvocationHandler invocationHandler) throws UnmarshalException {
        try {
            proxyHandlerField.set(objectToFill, invocationHandler);
        } catch (IllegalAccessException e) {
            throw new UnmarshalException("Cannot set Proxy.h", e);
        }
    }

    private static class PlaceholderInvocationHandler implements InvocationHandler {
        /** {@inheritDoc} */
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("equals".equals(method.getName()) && method.getParameterTypes().length == 1
                    && method.getParameterTypes()[0] == Object.class) {
                return proxy == args[0];
            }
            if ("hashCode".equals(method.getName()) && noArgs(method)) {
                return hashCode();
            }
            if ("toString".equals(method.getName()) && noArgs(method)) {
                return "Proxy with placeholder";
            }

            throw new UnsupportedOperationException("This is a dummy placeholder, but it was got an invocation on " + method);
        }

        private boolean noArgs(Method method) {
            return method.getParameterTypes().length == 0;
        }
    }
}
