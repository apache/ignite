package org.apache.ignite.kubernetes.configuration;

import org.apache.ignite.spi.IgniteSpiException;
import org.junit.Before;
import org.junit.Test;

/** Class checks required fields for {@link KubernetesConnectionConfiguration} */
public class KubernetesConnectionConfigurationTest {
    /** */
    private KubernetesConnectionConfiguration cfg;

    /** Fill all required fields. */
    @Before
    public void setup() {
        cfg = new KubernetesConnectionConfiguration();

        cfg.setAccountToken("accountToken");
        cfg.setMasterUrl("masterUrl");
        cfg.setNamespace("namespace");
        cfg.setServiceName("serviceName");
    }

    /** */
    @Test
    public void smoketest() {
        cfg.verify();
    }

    /** */
    @Test(expected = IgniteSpiException.class)
    public void testAccountNameIsRequired() {
        cfg.setAccountToken(null);
        cfg.verify();
    }

    /** */
    @Test(expected = IgniteSpiException.class)
    public void testMasterUrlIsRequired() {
        cfg.setMasterUrl(null);
        cfg.verify();
    }

    /** */
    @Test(expected = IgniteSpiException.class)
    public void testNamespaceIsRequired() {
        cfg.setNamespace(null);
        cfg.verify();
    }

    /** */
    @Test(expected = IgniteSpiException.class)
    public void testServiceNameIsRequired() {
        cfg.setServiceName("");
        cfg.verify();
    }
}
