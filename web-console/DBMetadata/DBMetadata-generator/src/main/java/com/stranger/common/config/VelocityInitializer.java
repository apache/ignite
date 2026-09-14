package com.stranger.common.config;

import java.util.Properties;
import org.apache.velocity.app.Velocity;

public class VelocityInitializer {
    public static void initVelocity() {
        Properties p = new Properties();
        try {
            p.setProperty("resource.loader.file.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
            p.setProperty("resource.default_encoding", "UTF-8");
            Velocity.init(p);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
