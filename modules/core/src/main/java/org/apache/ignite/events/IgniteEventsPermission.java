package org.apache.ignite.events;

import java.security.BasicPermission;

public class IgniteEventsPermission extends BasicPermission {

    private static final long serialVersionUID = 0L;

    public static final String ENABLED = "enable";

    public static final String DISABLED = "disable";

    public IgniteEventsPermission(String name) {
        super(name);
    }

}
