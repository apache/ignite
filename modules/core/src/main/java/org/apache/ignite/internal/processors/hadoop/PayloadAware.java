package org.apache.ignite.internal.processors.hadoop;

/**
 * Created by ivan on 22.12.15.
 */
public interface PayloadAware <P> {

    public P getPayload();
}
