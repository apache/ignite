package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;

//tag::bean[]
public class MyLifecycleBean implements LifecycleBean {
    @IgniteInstanceResource
    public Ignite ignite;

    @Override
    public void onLifecycleEvent(LifecycleEventType evt) {
        if (evt == LifecycleEventType.AFTER_NODE_START) {

            System.out.format("After the node (consistentId = %s) starts.\n", ignite.cluster().node().consistentId());

        }
    }
}

//end::bean[]