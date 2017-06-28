package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for ENUM with declared body.
 */
public class MarshallerEnumTest extends GridCommonAbstractTest {
    /** */
    private boolean compactFooter() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimized() throws Exception {
        final MarshallerContextTestImpl ctx = new MarshallerContextTestImpl();
        ctx.registerClassName((byte)0, 1, Application.class.getName());
        ctx.registerClassName((byte)0, 2, Color.class.getName());

        OptimizedMarshaller marsh = new OptimizedMarshaller();
        marsh.setContext(ctx);
        checkEnum(marsh);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJdk() throws Exception {
        final MarshallerContextTestImpl ctx = new MarshallerContextTestImpl();
        ctx.registerClassName((byte)0, 1, Application.class.getName());
        ctx.registerClassName((byte)0, 2, Color.class.getName());

        JdkMarshaller marsh = new JdkMarshaller();
        marsh.setContext(ctx);
        checkEnum(marsh);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinary() throws Exception {
        final MarshallerContextTestImpl ctx = new MarshallerContextTestImpl();
        ctx.registerClassName((byte)0, 1, Application.class.getName());
        ctx.registerClassName((byte)0, 2, Color.class.getName());

        BinaryMarshaller marsh = binaryMarshaller();
        marsh.setContext(ctx);
        checkEnum(marsh);
    }

    /** */
    private void checkEnum(final Marshaller marsh) throws IgniteCheckedException {
        Application app = new Application(1L, "app 1", Color.BLACK);

        final byte[] marshal = marsh.marshal(app);
        final Object restored = marsh.unmarshal(marshal, null);

        assertTrue(restored instanceof Application);

        app = (Application)restored;

        assertEquals(1, app.id);
        assertEquals(Color.BLACK.ordinal(), app.type.ordinal());
        assertEquals(Color.BLACK, app.type);
        assertTrue(app.type == Color.BLACK);
    }

    /** */
    protected BinaryMarshaller binaryMarshaller() throws IgniteCheckedException {
        return binaryMarshaller(null, null, null, null, null);
    }

    /**
     * @return Binary marshaller.
     */
    protected BinaryMarshaller binaryMarshaller(
        BinaryNameMapper nameMapper,
        BinaryIdMapper mapper,
        BinarySerializer serializer,
        Collection<BinaryTypeConfiguration> cfgs,
        Collection<String> excludedClasses
    ) throws IgniteCheckedException {
        IgniteConfiguration iCfg = new IgniteConfiguration();

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setNameMapper(nameMapper);
        bCfg.setIdMapper(mapper);
        bCfg.setSerializer(serializer);
        bCfg.setCompactFooter(compactFooter());

        bCfg.setTypeConfigurations(cfgs);

        iCfg.setBinaryConfiguration(bCfg);

        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), iCfg, new NullLogger());

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(new MarshallerContextTestImpl(null, excludedClasses));

        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, iCfg);

        return marsh;
    }

    /** */
    private static class Application implements Serializable {
        /** */
        private long id;

        /** */
        private String name;

        /** */
        private Color type;

        /** */
        Application(final long id, final String name, final Color type) {
            this.id = id;
            this.name = name;
            this.type = type;
        }
    }

    /** */
    public enum Color {
        WHITE {
            @Override boolean isSupported() {
                return false;
            }
        },
        BLACK {
            @Override boolean isSupported() {
                return false;
            }
        };

        abstract boolean isSupported();
    }
}