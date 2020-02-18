package org.apache.ignite.internal.processors.service.inner;

import org.apache.ignite.services.ServiceContext;

/** {@inheritDoc} */
public class NamingServiceImpl implements NamingService {
    /** {@inheritDoc} */
    @Override public int process(org.apache.ignite.internal.processors.service.inner.impl.Param param) {
        return param.value();
    }

    /** {@inheritDoc} */
    @Override public int process(org.apache.ignite.internal.processors.service.inner.experimental.Param param) {
        return param.value();
    }

    /** */
    @Override public void cancel(ServiceContext ctx) {

    }

    /** */
    @Override public void init(ServiceContext ctx) throws Exception {

    }

    /** */
    @Override public void execute(ServiceContext ctx) throws Exception {

    }
}
