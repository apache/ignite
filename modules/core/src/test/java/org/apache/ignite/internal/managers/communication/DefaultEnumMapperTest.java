package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.plugin.extensions.communication.mappers.DefaultEnumMapper;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** */
public class DefaultEnumMapperTest {
    /** */
    private final TransactionIsolation[] txIsolationVals = TransactionIsolation.values();

    /** */
    @Test
    public void testEncode() {
        assertEquals(-1, DefaultEnumMapper.INSTANCE.encode(null));

        for (TransactionIsolation txIsolation : txIsolationVals)
            assertEquals(txIsolation.ordinal(), DefaultEnumMapper.INSTANCE.encode(txIsolation));
    }

    /** */
    @Test
    public void testDecode() {
        assertNull(DefaultEnumMapper.INSTANCE.decode(txIsolationVals, (byte)-1));

        for (TransactionIsolation txIsolation : txIsolationVals) {
            assertEquals(txIsolationVals[txIsolation.ordinal()],
                DefaultEnumMapper.INSTANCE.decode(txIsolationVals, (byte)txIsolation.ordinal()));
        }

        Throwable ex = GridTestUtils.assertThrowsWithCause(
            () -> DefaultEnumMapper.INSTANCE.decode(txIsolationVals, (byte)txIsolationVals.length),
            IllegalArgumentException.class
        );

        assertEquals("Enum code " + txIsolationVals.length + " is out of range for enum type " +
                TransactionIsolation.class.getName(),
            ex.getMessage());
    }
}
