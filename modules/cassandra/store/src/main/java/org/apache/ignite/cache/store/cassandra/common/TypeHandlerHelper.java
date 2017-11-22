package org.apache.ignite.cache.store.cassandra.common;

import com.google.common.base.Strings;
import org.apache.ignite.IgniteException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TypeHandlerHelper {
    private static final Map<String, TypeHandler> cacheHandler = new ConcurrentHashMap<>();

    public static TypeHandler getInstanceFromClassName(String handlerClazzName) {
        handlerClazzName = Strings.nullToEmpty(handlerClazzName).trim();

        if (!handlerClazzName.isEmpty()) {
            TypeHandler typeHandler;
            typeHandler = cacheHandler.get(handlerClazzName);
            if (typeHandler != null) {
                return typeHandler;
            }
            try {
                Class<?> handlerClass = Class.forName(handlerClazzName);
                if (handlerClass.isAssignableFrom(TypeHandler.class)) {
                    throw new IgniteException("Handler class do not implement " + TypeHandler.class.getName());
                }
                typeHandler = (TypeHandler) handlerClass.newInstance();

                cacheHandler.put(handlerClazzName, typeHandler);

                return typeHandler;
            } catch (IgniteException e) {
                throw e;
            } catch (Exception e) {
                throw new IgniteException("Error when try get handler class: " + handlerClazzName);
            }
        }
        return null;
    }
}
