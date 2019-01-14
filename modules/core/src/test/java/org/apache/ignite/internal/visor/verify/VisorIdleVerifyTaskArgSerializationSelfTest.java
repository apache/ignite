package org.apache.ignite.internal.visor.verify;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test checks backward compatibility for {@link VisorIdleVerifyTaskArg}.
 */
@RunWith(JUnit4.class)
public class VisorIdleVerifyTaskArgSerializationSelfTest {
    /** Current protocol version. */
    private static final byte CURRENT_PROTO_VER = 3;

    /** Caches. */
    private static final Set<String> CACHES = Collections.singleton("cache1");

    /** Exclude caches. */
    private static final Set<String> EXCLUDE_CACHES = Collections.singleton("ex_cache1");

    /** Golden argument for serialization. */
    private static final VisorIdleVerifyTaskArg GOLDEN_ARG = new VisorIdleVerifyTaskArg(CACHES, EXCLUDE_CACHES, true);

    /** Deserialized argument */
    private VisorIdleVerifyTaskArg arg;

    /** Byte array with serialized data. */
    private ByteArrayOutputStream bytes;

    /** */
    @Before
    public void beforeTest() throws IOException {
        arg = new VisorIdleVerifyTaskArg();

        bytes = new ByteArrayOutputStream();

        try(ObjectOutputStream stream = new ObjectOutputStream(bytes)) {
            GOLDEN_ARG.writeExternalData(stream);
        }
    }

    /** */
    @Test
    public void protocolVersionNotChanged() {
        byte ver = arg.getProtocolVersion();

        assertEquals(
            "You should add tests for new protocol version " + ver + " and update CURRENT_PROTO_VER in test!",
            CURRENT_PROTO_VER,
            ver
        );
    }

    /** */
    @Test
    public void deserializeWithVersion1() throws IOException, ClassNotFoundException {
        readExternal((byte)1);

        checkFieldsForVersion1();

        assertNull(arg.getExcludeCaches());
        assertFalse(arg.isCheckCrc());
    }

    /** */
    @Test
    public void deserializeWithVersion2() throws IOException, ClassNotFoundException {
        readExternal((byte)2);

        checkFieldsForVersion2();

        assertFalse(arg.isCheckCrc());
    }

    /** */
    @Test
    public void deserializeWithVersion3() throws IOException, ClassNotFoundException {
        readExternal((byte)3);

        checkFieldsForVersion3();
    }

    /**
     * Check's correct fields deserialization for protocol version 3.
     */
    private void checkFieldsForVersion3() {
        checkFieldsForVersion2();

        assertTrue(arg.isCheckCrc());
    }

    /**
     * Check's correct fields deserialization for protocol version 2.
     */
    private void checkFieldsForVersion2() {
        checkFieldsForVersion1();

        assertEquals(GOLDEN_ARG.getExcludeCaches(), arg.getExcludeCaches());
    }

    /**
     * Check's correct fields deserialization for protocol version 1.
     */
    private void checkFieldsForVersion1() {
        assertEquals(GOLDEN_ARG.getCaches(), arg.getCaches());
    }

    /**
     * Read external data with passed protocol version.
     *
     * @param ver Version.
     */
    private void readExternal(byte ver) throws IOException, ClassNotFoundException {
        assert ver > 0 : ver;
        assert ver <= CURRENT_PROTO_VER: ver;

        try(ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            arg.readExternalData(ver, stream);
        }
    }
}
