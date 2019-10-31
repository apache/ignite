/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 */
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
package org.h2.test.trace;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

/**
 * A statement in a Java-style log file.
 */
class Statement {
    private final Player player;
    private boolean assignment;
    private boolean staticCall;
    private String assignClass;
    private String assignVariable;
    private String staticCallClass;
    private String objectName;
    private Object object;
    private String methodName;
    private Arg[] args;
    private Class<?> returnClass;

    Statement(Player player) {
        this.player = player;
    }

    /**
     * Execute the statement.
     *
     * @return the object returned if this was a method call
     */
    Object execute() throws Exception {
        if (object == player) {
            // there was an exception previously
            player.trace("> " + assignVariable + " not set");
            if (assignment) {
                player.assign(assignVariable, player);
            }
            return null;
        }
        Class<?> clazz;
        if (staticCall) {
            clazz = Player.getClass(staticCallClass);
        } else {
            clazz = object.getClass();
        }
        Class<?>[] parameterTypes = new Class[args.length];
        Object[] parameters = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            Arg arg = args[i];
            arg.execute();
            parameterTypes[i] = arg.getValueClass();
            parameters[i] = arg.getValue();
        }
        Method method = clazz.getMethod(methodName, parameterTypes);
        returnClass = method.getReturnType();
        try {
            Object obj = method.invoke(object, parameters);
            if (assignment) {
                player.assign(assignVariable, obj);
            }
            return obj;
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();
            player.trace("> " + t.toString());
            if (assignment) {
                player.assign(assignVariable, player);
            }
        }
        return null;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        if (assignment) {
            buff.append(assignClass);
            buff.append(' ');
            buff.append(assignVariable);
            buff.append('=');
        }
        if (staticCall) {
            buff.append(staticCallClass);
        } else {
            buff.append(objectName);
        }
        buff.append('.');
        buff.append(methodName);
        buff.append('(');
        for (int i = 0; args != null && i < args.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(args[i].toString());
        }
        buff.append(");");
        return buff.toString();
    }

    Class<?> getReturnClass() {
        return returnClass;
    }

    /**
     * This statement is an assignment.
     *
     * @param className the class of the variable
     * @param variableName the variable name
     */
    void setAssign(String className, String variableName) {
        this.assignment = true;
        this.assignClass = className;
        this.assignVariable = variableName;
    }

    /**
     * This statement is a static method call.
     *
     * @param className the class name
     */
    void setStaticCall(String className) {
        this.staticCall = true;
        this.staticCallClass = className;
    }

    /**
     * This statement is a method call, and the result is assigned to a
     * variable.
     *
     * @param variableName the variable name
     * @param object the object
     * @param methodName the method name
     */
    void setMethodCall(String variableName, Object object, String methodName) {
        this.objectName = variableName;
        this.object = object;
        this.methodName = methodName;
    }

    public void setArgs(ArrayList<Arg> list) {
        args = list.toArray(new Arg[0]);
    }
}
