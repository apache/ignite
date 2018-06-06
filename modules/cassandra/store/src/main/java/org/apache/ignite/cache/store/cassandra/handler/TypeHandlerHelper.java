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

package org.apache.ignite.cache.store.cassandra.handler;

import com.google.common.base.Strings;
import org.apache.ignite.IgniteException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper class providing methods for managing type handler instances.
 */
public class TypeHandlerHelper {
    private static final Map<Class<?>, TypeHandler> cacheHandler = new ConcurrentHashMap<>();

    /**
     * Get instance of type handler from class name. <br/>
     * If object does not exist, it will be created and cached.
     *
     * @param handlerClazzName type handler class name
     * @return instance of type handler
     */
    public static TypeHandler getInstanceFromClassName(String handlerClazzName) {
        handlerClazzName = Strings.nullToEmpty(handlerClazzName).trim();
        if (!handlerClazzName.isEmpty()) {
            try {
                Class<?> handlerClass = Class.forName(handlerClazzName);
                return getInstanceFromClass(handlerClass);
            } catch (IgniteException e) {
                throw e;
            } catch (Exception e) {
                throw new IgniteException("Error when try get handler class: " + handlerClazzName);
            }
        }
        return null;
    }

    /**
     * Get instance of type handler from class. <br/>
     * If object does not exist, it will be created and cached.
     *
     * @param handlerClass type handler class
     * @return instance of type handler
     */
    public static TypeHandler getInstanceFromClass(Class<?> handlerClass) {
        TypeHandler typeHandler;
        typeHandler = cacheHandler.get(handlerClass);
        if (typeHandler != null) {
            return typeHandler;
        }
        try {
            if (handlerClass.isAssignableFrom(TypeHandler.class)) {
                throw new IgniteException("Handler class does not implement " + TypeHandler.class.getName());
            }
            typeHandler = (TypeHandler) handlerClass.newInstance();

            cacheHandler.put(handlerClass, typeHandler);

            return typeHandler;
        } catch (IgniteException e) {
            throw e;
        } catch (Exception e) {
            throw new IgniteException("Error when try get handler class: " + handlerClass.getName());
        }
    }
}
