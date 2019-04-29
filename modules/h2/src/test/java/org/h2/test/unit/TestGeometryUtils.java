/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import static org.h2.util.geometry.GeometryUtils.DIMENSION_SYSTEM_XY;
import static org.h2.util.geometry.GeometryUtils.DIMENSION_SYSTEM_XYM;
import static org.h2.util.geometry.GeometryUtils.DIMENSION_SYSTEM_XYZ;
import static org.h2.util.geometry.GeometryUtils.DIMENSION_SYSTEM_XYZM;
import static org.h2.util.geometry.GeometryUtils.GEOMETRY_COLLECTION;
import static org.h2.util.geometry.GeometryUtils.M;
import static org.h2.util.geometry.GeometryUtils.MAX_X;
import static org.h2.util.geometry.GeometryUtils.MAX_Y;
import static org.h2.util.geometry.GeometryUtils.MIN_X;
import static org.h2.util.geometry.GeometryUtils.MIN_Y;
import static org.h2.util.geometry.GeometryUtils.X;
import static org.h2.util.geometry.GeometryUtils.Y;
import static org.h2.util.geometry.GeometryUtils.Z;

import java.io.ByteArrayOutputStream;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.util.StringUtils;
import org.h2.util.geometry.EWKBUtils;
import org.h2.util.geometry.EWKBUtils.EWKBTarget;
import org.h2.util.geometry.EWKTUtils;
import org.h2.util.geometry.EWKTUtils.EWKTTarget;
import org.h2.util.geometry.GeometryUtils;
import org.h2.util.geometry.GeometryUtils.DimensionSystemTarget;
import org.h2.util.geometry.GeometryUtils.EnvelopeAndDimensionSystemTarget;
import org.h2.util.geometry.GeometryUtils.Target;
import org.h2.util.geometry.JTSUtils;
import org.h2.util.geometry.JTSUtils.GeometryTarget;
import org.h2.value.ValueGeometry;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

/**
 * Tests the classes from org.h2.util.geometry package.
 */
public class TestGeometryUtils extends TestBase {

    private static final byte[][] NON_FINITE = { //
            // XY
            StringUtils.convertHexToBytes("0000000001" //
                    + "0000000000000000" //
                    + "7ff8000000000000"), //
            // XY
            StringUtils.convertHexToBytes("0000000001" //
                    + "7ff8000000000000" //
                    + "0000000000000000"), //
            // XYZ
            StringUtils.convertHexToBytes("0080000001" //
                    + "0000000000000000" //
                    + "0000000000000000" //
                    + "7ff8000000000000"), //
            // XYM
            StringUtils.convertHexToBytes("0040000001" //
                    + "0000000000000000" //
                    + "0000000000000000" //
                    + "7ff8000000000000") };

    private static final int[] NON_FINITE_DIMENSIONS = { //
            DIMENSION_SYSTEM_XY, //
            DIMENSION_SYSTEM_XY, //
            DIMENSION_SYSTEM_XYZ, //
            DIMENSION_SYSTEM_XYM };

    private static final String MIXED_WKT = "LINESTRING (1 2, 3 4 5)";

    private static final byte[] MIXED_WKB = StringUtils.convertHexToBytes(""
            // BOM (BigEndian)
            + "00"
            // Z | LINESTRING
            + "80000002"
            // 2 items
            + "00000002"
            // 1.0
            + "3ff0000000000000"
            // 2.0
            + "4000000000000000"
            // NaN
            + "7ff8000000000000"
            // 3.0
            + "4008000000000000"
            // 4.0
            + "4010000000000000"
            // 5.0
            + "4014000000000000");

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testPoint();
        testLineString();
        testPolygon();
        testMultiPoint();
        testMultiLineString();
        testMultiPolygon();
        testGeometryCollection();
        testEmptyPoint();
        testDimensionM();
        testDimensionZM();
        testFiniteOnly();
        testSRID();
        testIntersectionAndUnion();
        testMixedGeometries();
    }

    private void testPoint() throws Exception {
        testGeometry("POINT (1 2)", 2);
        testGeometry("POINT (-1.3 15)", 2);
        testGeometry("POINT (-1E32 1.000001)", "POINT (-1E32 1.000001)",
                "POINT (-100000000000000000000000000000000 1.000001)", 2);
        testGeometry("POINT Z (2.7 -3 34)", 3);
        assertEquals("POINT Z (1 2 3)", EWKTUtils.ewkb2ewkt(EWKTUtils.ewkt2ewkb("POINTZ(1 2 3)")));
        assertEquals("POINT Z (1 2 3)", EWKTUtils.ewkb2ewkt(EWKTUtils.ewkt2ewkb("pointz(1 2 3)")));
    }

    private void testLineString() throws Exception {
        testGeometry("LINESTRING (-1 -2, 10 1, 2 20)", 2);
        testGeometry("LINESTRING (1 2, 1 3)", 2);
        testGeometry("LINESTRING (1 2, 2 2)", 2);
        testGeometry("LINESTRING EMPTY", 2);
        testGeometry("LINESTRING Z (-1 -2 -3, 10 15.7 3)", 3);
    }

    private void testPolygon() throws Exception {
        testGeometry("POLYGON ((-1 -2, 10 1, 2 20, -1 -2))", 2);
        testGeometry("POLYGON EMPTY", 2);
        testGeometry("POLYGON ((-1 -2, 10 1, 2 20, -1 -2), (0.5 0.5, 1 0.5, 1 1, 0.5 0.5))", 2);
        // TODO is EMPTY inner ring valid?
        testGeometry("POLYGON ((-1 -2, 10 1, 2 20, -1 -2), EMPTY)", 2);
        testGeometry("POLYGON Z ((-1 -2 7, 10 1 7, 2 20 7, -1 -2 7), (0.5 0.5 7, 1 0.5 7, 1 1 7, 0.5 0.5 7))", 3);
    }

    private void testMultiPoint() throws Exception {
        testGeometry("MULTIPOINT ((1 2), (3 4))", 2);
        // Alternative syntax
        testGeometry("MULTIPOINT (1 2, 3 4)", "MULTIPOINT ((1 2), (3 4))", "MULTIPOINT ((1 2), (3 4))", 2);
        testGeometry("MULTIPOINT (1 2)", "MULTIPOINT ((1 2))", "MULTIPOINT ((1 2))", 2);
        testGeometry("MULTIPOINT EMPTY", 2);
        testGeometry("MULTIPOINT Z ((1 2 0.5), (3 4 -3))", 3);
    }

    private void testMultiLineString() throws Exception {
        testGeometry("MULTILINESTRING ((1 2, 3 4, 5 7))", 2);
        testGeometry("MULTILINESTRING ((1 2, 3 4, 5 7), (-1 -1, 0 0, 2 2, 4 6.01))", 2);
        testGeometry("MULTILINESTRING EMPTY", 2);
        testGeometry("MULTILINESTRING Z ((1 2 0.5, 3 4 -3, 5 7 10))", 3);
    }

    private void testMultiPolygon() throws Exception {
        testGeometry("MULTIPOLYGON (((-1 -2, 10 1, 2 20, -1 -2)))", 2);
        testGeometry("MULTIPOLYGON (((-1 -2, 10 1, 2 20, -1 -2)), ((1 2, 2 2, 3 3, 1 2)))", 2);
        testGeometry("MULTIPOLYGON EMPTY", 2);
        testGeometry("MULTIPOLYGON (((-1 -2, 10 1, 2 20, -1 -2), (0.5 0.5, 1 0.5, 1 1, 0.5 0.5)))", 2);
        testGeometry("MULTIPOLYGON Z (((-1 -2 7, 10 1 7, 2 20 7, -1 -2 7), (0.5 1 7, 1 0.5 7, 1 1 7, 0.5 1 7)))", 3);
    }

    private void testGeometryCollection() throws Exception {
        testGeometry("GEOMETRYCOLLECTION (POINT (1 2))", 2);
        testGeometry("GEOMETRYCOLLECTION (POINT (1 2), " //
                + "MULTILINESTRING ((1 2, 3 4, 5 7), (-1 -1, 0 0, 2 2, 4 6.01)), " //
                + "POINT (100 130))", 2);
        testGeometry("GEOMETRYCOLLECTION EMPTY", 2);
        testGeometry(
                "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 3)), MULTIPOINT ((4 8)), GEOMETRYCOLLECTION EMPTY)",
                2);
        testGeometry("GEOMETRYCOLLECTION Z (POINT Z (1 2 3))", 3);
    }

    private void testGeometry(String wkt, int numOfDimensions) throws Exception {
        testGeometry(wkt, wkt, wkt, numOfDimensions);
    }

    private void testGeometry(String wkt, String h2Wkt, String jtsWkt, int numOfDimensions) throws Exception {
        Geometry geometryFromJTS = new WKTReader().read(wkt);
        byte[] wkbFromJTS = new WKBWriter(numOfDimensions).write(geometryFromJTS);

        // Test WKB->WKT conversion
        assertEquals(h2Wkt, EWKTUtils.ewkb2ewkt(wkbFromJTS));

        // Test WKT->WKB conversion
        assertEquals(wkbFromJTS, EWKTUtils.ewkt2ewkb(wkt));

        // Test WKB->WKB no-op normalization
        assertEquals(wkbFromJTS, EWKBUtils.ewkb2ewkb(wkbFromJTS));

        // Test WKB->Geometry conversion
        Geometry geometryFromH2 = JTSUtils.ewkb2geometry(wkbFromJTS);
        String got = new WKTWriter(numOfDimensions).write(geometryFromH2);
        if (!jtsWkt.equals(got)) {
            if (!jtsWkt.replaceAll(" Z", "").equals(got)) { // JTS 1.15
                assertEquals(jtsWkt.replaceAll(" Z ", " Z"), got); // JTS 1.16
            }
        }

        // Test Geometry->WKB conversion
        assertEquals(wkbFromJTS, JTSUtils.geometry2ewkb(geometryFromJTS));

        // Test Envelope
        Envelope envelopeFromJTS = geometryFromJTS.getEnvelopeInternal();
        testEnvelope(envelopeFromJTS, GeometryUtils.getEnvelope(wkbFromJTS));
        EnvelopeAndDimensionSystemTarget target = new EnvelopeAndDimensionSystemTarget();
        EWKBUtils.parseEWKB(wkbFromJTS, target);
        testEnvelope(envelopeFromJTS, target.getEnvelope());

        // Test dimensions
        int expectedDimensionSystem = numOfDimensions > 2 ? GeometryUtils.DIMENSION_SYSTEM_XYZ
                : GeometryUtils.DIMENSION_SYSTEM_XY;
        testDimensions(expectedDimensionSystem, wkbFromJTS);

        testValueGeometryProperties(wkbFromJTS);
    }

    private void testEnvelope(Envelope envelopeFromJTS, double[] envelopeFromH2) {
        if (envelopeFromJTS.isNull()) {
            assertNull(envelopeFromH2);
            assertNull(EWKBUtils.envelope2wkb(envelopeFromH2));
        } else {
            assertEquals(envelopeFromJTS.getMinX(), envelopeFromH2[0]);
            assertEquals(envelopeFromJTS.getMaxX(), envelopeFromH2[1]);
            assertEquals(envelopeFromJTS.getMinY(), envelopeFromH2[2]);
            assertEquals(envelopeFromJTS.getMaxY(), envelopeFromH2[3]);
            assertEquals(new WKBWriter(2).write(new GeometryFactory().toGeometry(envelopeFromJTS)),
                    EWKBUtils.envelope2wkb(envelopeFromH2));
        }
    }

    private void testEmptyPoint() {
        String ewkt = "POINT EMPTY";
        byte[] ewkb = EWKTUtils.ewkt2ewkb(ewkt);
        assertEquals(StringUtils.convertHexToBytes("00000000017ff80000000000007ff8000000000000"), ewkb);
        assertEquals(ewkt, EWKTUtils.ewkb2ewkt(ewkb));
        assertNull(GeometryUtils.getEnvelope(ewkb));
        Point p = (Point) JTSUtils.ewkb2geometry(ewkb);
        assertTrue(p.isEmpty());
        assertEquals(ewkt, new WKTWriter().write(p));
        assertEquals(ewkb, JTSUtils.geometry2ewkb(p));
    }

    private void testDimensionM() throws Exception {
        byte[] ewkb = EWKTUtils.ewkt2ewkb("POINT M (1 2 3)");
        assertEquals("POINT M (1 2 3)", EWKTUtils.ewkb2ewkt(ewkb));
        assertEquals("POINT M (1 2 3)", EWKTUtils.ewkb2ewkt(EWKTUtils.ewkt2ewkb("POINTM(1 2 3)")));
        assertEquals("POINT M (1 2 3)", EWKTUtils.ewkb2ewkt(EWKTUtils.ewkt2ewkb("pointm(1 2 3)")));
        Point p = (Point) JTSUtils.ewkb2geometry(ewkb);
        CoordinateSequence cs = p.getCoordinateSequence();
        testDimensionMCheckPoint(cs);
        assertEquals(ewkb, JTSUtils.geometry2ewkb(p));
        testDimensions(GeometryUtils.DIMENSION_SYSTEM_XYM, ewkb);
        testValueGeometryProperties(ewkb);

        if (JTSUtils.M_IS_SUPPORTED) {
            p = (Point) new WKTReader().read("POINT M (1 2 3)");
            cs = p.getCoordinateSequence();
            assertEquals(3, cs.getDimension());
            assertEquals(1, (int) cs.getClass().getMethod("getMeasures").invoke(cs));
            assertEquals(1, cs.getOrdinate(0, 0));
            assertEquals(2, cs.getOrdinate(0, 1));
            assertEquals(3, cs.getOrdinate(0, 2));
            ewkb = JTSUtils.geometry2ewkb(p);
            assertEquals("POINT M (1 2 3)", EWKTUtils.ewkb2ewkt(ewkb));
            p = (Point) JTSUtils.ewkb2geometry(ewkb);
            cs = p.getCoordinateSequence();
            testDimensionMCheckPoint(cs);
            assertEquals(1, (int) cs.getClass().getMethod("getMeasures").invoke(cs));
        }
    }

    private void testDimensionMCheckPoint(CoordinateSequence cs) {
        assertEquals(4, cs.getDimension());
        assertEquals(1, cs.getOrdinate(0, X));
        assertEquals(2, cs.getOrdinate(0, Y));
        assertEquals(Double.NaN, cs.getOrdinate(0, Z));
        assertEquals(3, cs.getOrdinate(0, M));
    }

    private void testDimensionZM() throws Exception {
        byte[] ewkb = EWKTUtils.ewkt2ewkb("POINT ZM (1 2 3 4)");
        assertEquals("POINT ZM (1 2 3 4)", EWKTUtils.ewkb2ewkt(ewkb));
        assertEquals("POINT ZM (1 2 3 4)", EWKTUtils.ewkb2ewkt(EWKTUtils.ewkt2ewkb("POINTZM(1 2 3 4)")));
        assertEquals("POINT ZM (1 2 3 4)", EWKTUtils.ewkb2ewkt(EWKTUtils.ewkt2ewkb("pointzm(1 2 3 4)")));
        Point p = (Point) JTSUtils.ewkb2geometry(ewkb);
        CoordinateSequence cs = p.getCoordinateSequence();
        testDimensionZMCheckPoint(cs);
        assertEquals(ewkb, JTSUtils.geometry2ewkb(p));
        testDimensions(GeometryUtils.DIMENSION_SYSTEM_XYZM, ewkb);
        testValueGeometryProperties(ewkb);

        if (JTSUtils.M_IS_SUPPORTED) {
            p = (Point) new WKTReader().read("POINT ZM (1 2 3 4)");
            cs = p.getCoordinateSequence();
            testDimensionZMCheckPoint(cs);
            assertEquals(1, (int) cs.getClass().getMethod("getMeasures").invoke(cs));
            ewkb = JTSUtils.geometry2ewkb(p);
            assertEquals("POINT ZM (1 2 3 4)", EWKTUtils.ewkb2ewkt(ewkb));
            p = (Point) JTSUtils.ewkb2geometry(ewkb);
            cs = p.getCoordinateSequence();
            testDimensionZMCheckPoint(cs);
            assertEquals(1, (int) cs.getClass().getMethod("getMeasures").invoke(cs));
        }
    }

    private void testDimensionZMCheckPoint(CoordinateSequence cs) {
        assertEquals(4, cs.getDimension());
        assertEquals(1, cs.getOrdinate(0, X));
        assertEquals(2, cs.getOrdinate(0, Y));
        assertEquals(3, cs.getOrdinate(0, Z));
        assertEquals(4, cs.getOrdinate(0, M));
    }

    private void testValueGeometryProperties(byte[] ewkb) {
        ValueGeometry vg = ValueGeometry.getFromEWKB(ewkb);
        DimensionSystemTarget target = new DimensionSystemTarget();
        EWKBUtils.parseEWKB(ewkb, target);
        int dimensionSystem = target.getDimensionSystem();
        assertEquals(dimensionSystem, vg.getDimensionSystem());
        String formattedType = EWKTUtils.formatGeometryTypeAndDimensionSystem(vg.getTypeAndDimensionSystem());
        assertTrue(EWKTUtils.ewkb2ewkt(ewkb).startsWith(formattedType));
        switch (dimensionSystem) {
        case DIMENSION_SYSTEM_XY:
            assertTrue(formattedType.indexOf(' ') < 0);
            break;
        case DIMENSION_SYSTEM_XYZ:
            assertTrue(formattedType.endsWith(" Z"));
            break;
        case DIMENSION_SYSTEM_XYM:
            assertTrue(formattedType.endsWith(" M"));
            break;
        case DIMENSION_SYSTEM_XYZM:
            assertTrue(formattedType.endsWith(" ZM"));
            break;
        }
        assertEquals(vg.getTypeAndDimensionSystem(), vg.getGeometryType() + vg.getDimensionSystem() * 1_000);
        assertEquals(0, vg.getSRID());
    }

    private void testFiniteOnly() {
        for (int i = 0; i < NON_FINITE.length; i++) {
            testFiniteOnly(NON_FINITE[i], new EWKBTarget(new ByteArrayOutputStream(), NON_FINITE_DIMENSIONS[i]));
        }
        for (int i = 0; i < NON_FINITE.length; i++) {
            testFiniteOnly(NON_FINITE[i], new EWKTTarget(new StringBuilder(), NON_FINITE_DIMENSIONS[i]));
        }
        for (int i = 0; i < NON_FINITE.length; i++) {
            testFiniteOnly(NON_FINITE[i], new GeometryTarget(NON_FINITE_DIMENSIONS[i]));
        }
    }

    private void testFiniteOnly(byte[] ewkb, Target target) {
        try {
            EWKBUtils.parseEWKB(ewkb, target);
            fail(target.getClass().getName() + ' ' + StringUtils.convertBytesToHex(ewkb));
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    private void testSRID() throws Exception {
        byte[] ewkb = EWKTUtils.ewkt2ewkb("SRID=10;GEOMETRYCOLLECTION (POINT (1 2))");
        assertEquals(StringUtils.convertHexToBytes(""
                // ******** Geometry collection ********
                // BOM (BigEndian)
                + "00"
                // Only top-level object has a SRID
                // type (SRID | POINT)
                + "20000007"
                // SRID = 10
                + "0000000a"
                // 1 item
                + "00000001"
                // ******** Point ********
                // BOM (BigEndian)
                + "00"
                // type (POINT)
                + "00000001"
                // 1.0
                + "3ff0000000000000"
                // 2.0
                + "4000000000000000"), ewkb);
        assertEquals("SRID=10;GEOMETRYCOLLECTION (POINT (1 2))", EWKTUtils.ewkb2ewkt(ewkb));
        GeometryCollection gc = (GeometryCollection) JTSUtils.ewkb2geometry(ewkb);
        assertEquals(10, gc.getSRID());
        assertEquals(10, gc.getGeometryN(0).getSRID());
        assertEquals(ewkb, JTSUtils.geometry2ewkb(gc));
        ValueGeometry vg = ValueGeometry.getFromEWKB(ewkb);
        assertEquals(10, vg.getSRID());
        assertEquals(GEOMETRY_COLLECTION, vg.getTypeAndDimensionSystem());
        assertEquals("SRID=-1;POINT EMPTY", EWKTUtils.ewkb2ewkt(EWKTUtils.ewkt2ewkb(" srid=-1  ; POINT  EMPTY ")));
    }

    private void testDimensions(int expected, byte[] ewkb) {
        DimensionSystemTarget dst = new DimensionSystemTarget();
        EWKBUtils.parseEWKB(ewkb, dst);
        assertEquals(expected, dst.getDimensionSystem());
        EnvelopeAndDimensionSystemTarget envelopeAndDimensionTarget = new EnvelopeAndDimensionSystemTarget();
        EWKBUtils.parseEWKB(ewkb, envelopeAndDimensionTarget);
        assertEquals(expected, envelopeAndDimensionTarget.getDimensionSystem());
    }

    private void testIntersectionAndUnion() {
        double[] zero = new double[4];
        assertFalse(GeometryUtils.intersects(null, null));
        assertFalse(GeometryUtils.intersects(null, zero));
        assertFalse(GeometryUtils.intersects(zero, null));
        assertNull(GeometryUtils.union(null, null));
        assertEquals(zero, GeometryUtils.union(null, zero));
        assertEquals(zero, GeometryUtils.union(zero, null));
        // These 30 values with fixed seed 0 are enough to cover all remaining
        // cases
        Random r = new Random(0);
        for (int i = 0; i < 30; i++) {
            double[] envelope1 = getEnvelope(r);
            double[] envelope2 = getEnvelope(r);
            Envelope e1 = convert(envelope1);
            Envelope e2 = convert(envelope2);
            assertEquals(e1.intersects(e2), GeometryUtils.intersects(envelope1, envelope2));
            e1.expandToInclude(e2);
            assertEquals(e1, convert(GeometryUtils.union(envelope1, envelope2)));
        }
    }

    private static Envelope convert(double[] envelope) {
        return new Envelope(envelope[MIN_X], envelope[MAX_X], envelope[MIN_Y], envelope[MAX_Y]);
    }

    private static double[] getEnvelope(Random r) {
        double minX = r.nextDouble();
        double maxX = r.nextDouble();
        if (minX > maxX) {
            double t = minX;
            minX = maxX;
            maxX = t;
        }
        double minY = r.nextDouble();
        double maxY = r.nextDouble();
        if (minY > maxY) {
            double t = minY;
            minY = maxY;
            maxY = t;
        }
        return new double[] { minX, maxX, minY, maxY };
    }

    private void testMixedGeometries() throws Exception {
        try {
            EWKTUtils.ewkt2ewkb(MIXED_WKT);
            fail();
        } catch (IllegalArgumentException ex) {
            // Expected
        }
        try {
            EWKTUtils.ewkb2ewkt(MIXED_WKB);
            fail();
        } catch (IllegalArgumentException ex) {
            // Expected
        }
        try {
            JTSUtils.ewkb2geometry(MIXED_WKB);
            fail();
        } catch (IllegalArgumentException ex) {
            // Expected
        }
        Geometry g = new WKTReader().read(MIXED_WKT);
        try {
            JTSUtils.geometry2ewkb(g);
            fail();
        } catch (IllegalArgumentException ex) {
            // Expected
        }
    }

}
