package org.apache.ignite.internal.processors.service.inner;

import org.apache.ignite.services.Service;

/** Gives almost same names (signatures) of the methods. For tests of name abbreviation. */
public interface NamingService extends Service {
    /**
     * A stub for overriding to distingush almost-same-params.
     *
     * @see org.apache.ignite.internal.processors.service.inner.impl.Param
     * @see org.apache.ignite.internal.processors.service.inner.experimental.Param
     */
    public int process(org.apache.ignite.internal.processors.service.inner.impl.Param param);

    /**
     * A stub for overriding to distingush almost-same-params.
     *
     * @see org.apache.ignite.internal.processors.service.inner.experimental.Param
     * @see org.apache.ignite.internal.processors.service.inner.impl.Param
     */
    public int process(org.apache.ignite.internal.processors.service.inner.experimental.Param param);
}
