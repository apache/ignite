package org.apache.ignite.internal.processors.service.inner;

import org.apache.ignite.services.Service;

/** Gives almost same names (signatures) of the methods. Destined for tests of names abbreviation. */
public interface NamingService extends Service {
    /** */
    public int process(org.apache.ignite.internal.processors.service.inner.impl.Param param);

    /** */
    public int process(org.apache.ignite.internal.processors.service.inner.experimental.Param param);
}
