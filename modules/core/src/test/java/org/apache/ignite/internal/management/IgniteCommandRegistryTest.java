package org.apache.ignite.internal.management;

import org.junit.Assert;
import org.junit.Test;

/** */
public class IgniteCommandRegistryTest {
    /** */
    @Test
    public void loadExternalCommandTest() {
        IgniteCommandRegistry registry = new IgniteCommandRegistry();
        Assert.assertNotNull(registry.command("Test"));
    }
}
