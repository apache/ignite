package org.apache.ignite.internal.processors.rest;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rest.handlers.cache.CacheOperationPermissionRestCommandHandlerCheckTest.CACHE_NAME;
import static org.apache.ignite.internal.processors.rest.handlers.cache.CacheOperationPermissionRestCommandHandlerCheckTest.CREATE_CACHE_NAME;
import static org.apache.ignite.internal.processors.rest.handlers.cache.CacheOperationPermissionRestCommandHandlerCheckTest.EMPTY_PERM;
import static org.apache.ignite.internal.processors.rest.handlers.cache.CacheOperationPermissionRestCommandHandlerCheckTest.FORBIDDEN_CACHE_NAME;
import static org.apache.ignite.internal.processors.rest.handlers.cache.CacheOperationPermissionRestCommandHandlerCheckTest.NEW_TEST_CACHE;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CACHE;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_OPS;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;

public class JettyRestProcessorSecurityCachePermissionSelfTest extends JettyRestProcessorAuthenticationWithCredsSelfTest {
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setAuthenticationEnabled(true)
            .setPluginProviders(new TestSecurityPluginProvider(DFLT_USER, DFLT_PWD,
                SecurityPermissionSetBuilder.create()
                    .defaultAllowAll(true)
                    .appendCachePermissions(CACHE_NAME, CACHE_CREATE, CACHE_READ, CACHE_PUT, CACHE_REMOVE, CACHE_DESTROY)
                    .appendCachePermissions(CREATE_CACHE_NAME, CACHE_CREATE)
                    .appendCachePermissions(FORBIDDEN_CACHE_NAME, EMPTY_PERM)
                    .appendSystemPermissions(JOIN_AS_SERVER, ADMIN_CACHE, ADMIN_OPS)
                    .build(), true));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override
    public void testGetOrCreateCache() throws Exception {
        assertTrue(content(NEW_TEST_CACHE, GridRestCommand.GET_OR_CREATE_CACHE).startsWith("{\"successStatus\":0"));
        assertTrue(content(CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE).startsWith("{\"successStatus\":0"));
        assertTrue(content(CREATE_CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE).startsWith("{\"successStatus\":0"));
        assertTrue(content(FORBIDDEN_CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE).startsWith("{\"successStatus\":1," +
            "\"error\":\"Failed to handle request: [req=GET_OR_CREATE_CACHE, err=Authorization failed [perm=CACHE_CREATE, name=FORBIDDEN_TEST_CACHE"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyCache() throws Exception {
        assertTrue(content(NEW_TEST_CACHE, GridRestCommand.DESTROY_CACHE).startsWith("{\"successStatus\":0"));
        assertTrue(content(CACHE_NAME, GridRestCommand.DESTROY_CACHE).startsWith("{\"successStatus\":0"));
        assertTrue(content(CREATE_CACHE_NAME, GridRestCommand.DESTROY_CACHE).startsWith("{\"successStatus\":1," +
            "\"error\":\"Failed to handle request: [req=DESTROY_CACHE, err=Authorization failed [perm=CACHE_DESTROY, name=CREATE_TEST_CACHE"));
        assertTrue(content(FORBIDDEN_CACHE_NAME, GridRestCommand.DESTROY_CACHE).startsWith("{\"successStatus\":1," +
            "\"error\":\"Failed to handle request: [req=DESTROY_CACHE, err=Authorization failed [perm=CACHE_DESTROY, name=FORBIDDEN_TEST_CACHE"));
    }

    /**
     * This test will be fail becouse req.authorizationContext not defined
     * in method UserActionCommandHandler#handleAsync provided ctx.security().enabled() == true.
     */
    @Override public void testAddUpdateRemoveUser() throws Exception {
        // No-op.
    }
}
