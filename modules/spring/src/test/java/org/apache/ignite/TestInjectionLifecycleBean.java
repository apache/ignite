package org.apache.ignite;

import org.apache.ignite.IgniteException;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.springframework.context.ApplicationContext;

import static org.junit.Assert.assertNotNull;

/** Lifecycle bean for testing. */
public class TestInjectionLifecycleBean implements LifecycleBean {
    /** */
    @SpringApplicationContextResource
    private ApplicationContext appCtx;

    /** Checks that context was injected. */
    public void checkState() {
        assertNotNull(appCtx);
    }

    /** */
    @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
        //No OP
    }
}
