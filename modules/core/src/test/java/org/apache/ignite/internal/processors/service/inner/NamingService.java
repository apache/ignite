package org.apache.ignite.internal.processors.service.inner;

import org.apache.ignite.services.Service;

/**
 * Gives almost same names (signatures) of the methods. For tests of name abbreviation.
 */
public interface NamingService extends Service {
    /**
     * A stub for overloading to distingush almost-same-params of the method.
     *
     * @see org.apache.ignite.internal.processors.service.inner.impl.Param
     */
    public int process(org.apache.ignite.internal.processors.service.inner.impl.Param param);

    /**
     * A stub for overloading to distingush almost-same-params of the method.
     *
     * @see org.apache.ignite.internal.processors.service.inner.experimental.Param
     */
    public int process(org.apache.ignite.internal.processors.service.inner.experimental.Param param);
}
