package org.apache.ignite.plugin;

import java.io.Serializable;

public class NodeValidationPluginConfiguration implements PluginConfiguration {
    private final String token;

    public NodeValidationPluginConfiguration(String token) {
        this.token = token;
    }

    public String getToken() {
        return token;
    }
}