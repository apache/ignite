/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.portable;

import org.apache.ignite.*;
import org.apache.ignite.internal.portable.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.portable.*;
import org.apache.ignite.portable.*;

/**
 *
 */
public class GridPortableCacheEntryMemorySizeSelfTest extends GridCacheEntryMemorySizeSelfTest {
    /** {@inheritDoc} */
    @Override protected Marshaller createMarshaller() throws IgniteCheckedException {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setContext(new MarshallerContextTestImpl(null));

        PortableContext pCtx = new PortableContext(new PortableMetaDataHandler() {
            @Override public void addMeta(int typeId, PortableMetaDataImpl meta) throws PortableException {
                // No-op
            }

            @Override public PortableMetadata metadata(int typeId) throws PortableException {
                return null;
            }
        }, null);

        IgniteUtils.invoke(PortableMarshaller.class, marsh, "setPortableContext", pCtx);

        return marsh;
    }
}
