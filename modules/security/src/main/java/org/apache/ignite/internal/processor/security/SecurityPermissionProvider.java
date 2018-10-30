package org.apache.ignite.internal.processor.security;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

public class SecurityPermissionProvider {

    private final static Map<Object, SecurityPermissionSet> PERMISSION_SET_MAP = new HashMap<>();

    public static SecurityPermissionSet permission(Object token) {
        return PERMISSION_SET_MAP.get(token);
    }

    public static void add(Object token, SecurityPermissionSet permissionSet) {
        PERMISSION_SET_MAP.put(token, permissionSet);
    }

    public static void clear(){
        PERMISSION_SET_MAP.clear();
    }

}
