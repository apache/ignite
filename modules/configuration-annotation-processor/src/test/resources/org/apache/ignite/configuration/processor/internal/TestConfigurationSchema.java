package org.apache.ignite.configuration.processor.internal;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;

@Config(value = "test", root = true)
public class TestConfigurationSchema {
    @Value
    private String value1;

    @Value
    private long primitiveLong;

    @Value
    private Long boxedLong;

    @Value
    private int primitiveInt;

    @Value
    private Integer boxedInt;
}