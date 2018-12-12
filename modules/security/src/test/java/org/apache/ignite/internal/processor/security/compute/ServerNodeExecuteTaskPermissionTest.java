package org.apache.ignite.internal.processor.security.compute;

/**
 * Test task execute permission for Executor Service on Server node.
 */
public class ServerNodeExecuteTaskPermissionTest extends ClientNodeExecuteTaskPermissionTest {
    /** {@inheritDoc} */
    @Override protected boolean isClient() {
        return false;
    }
}
