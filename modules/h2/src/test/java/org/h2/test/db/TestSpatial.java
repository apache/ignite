/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.util.Random;
import org.h2.api.Aggregate;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;
import org.h2.tools.SimpleRowSource;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * Spatial datatype and index tests.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class TestSpatial extends TestBase {

    private static final String URL = "spatial";

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        if (!config.mvStore && config.mvcc) {
            return;
        }
        if (config.memory && config.mvcc) {
            return;
        }
        if (DataType.GEOMETRY_CLASS != null) {
            deleteDb("spatial");
            testSpatial();
            deleteDb("spatial");
        }
    }

    private void testSpatial() throws SQLException {
        testBug1();
        testSpatialValues();
        testOverlap();
        testNotOverlap();
        testPersistentSpatialIndex();
        testSpatialIndexQueryMultipleTable();
        testIndexTransaction();
        testJavaAlias();
        testJavaAliasTableFunction();
        testMemorySpatialIndex();
        testGeometryDataType();
        testWKB();
        testValueConversion();
        testEquals();
        testTableFunctionGeometry();
        testHashCode();
        testAggregateWithGeometry();
        testTableViewSpatialPredicate();
        testValueGeometryScript();
        testInPlaceUpdate();
        testScanIndexOnNonSpatialQuery();
        testStoreCorruption();
        testExplainSpatialIndexWithPk();
        testNullableGeometry();
        testNullableGeometryDelete();
        testNullableGeometryInsert();
        testNullableGeometryUpdate();
        testIndexUpdateNullGeometry();
        testInsertNull();
        testSpatialIndexWithOrder();
    }

    private void testBug1() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(URL);
        Statement stat = conn.createStatement();

        stat.execute("CREATE TABLE VECTORS (ID INTEGER NOT NULL, GEOM GEOMETRY, S INTEGER)");
        stat.execute("INSERT INTO VECTORS(ID, GEOM, S) " +
                "VALUES(0, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))', 1)");

        stat.executeQuery("select * from (select * from VECTORS) WHERE S=1 " +
                "AND GEOM && 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'");
        conn.close();
        deleteDb("spatial");
    }

    private void testHashCode() {
        ValueGeometry geomA = ValueGeometry
                .get("POLYGON ((67 13 6, 67 18 5, 59 18 4, 59 13 6,  67 13 6))");
        ValueGeometry geomB = ValueGeometry
                .get("POLYGON ((67 13 6, 67 18 5, 59 18 4, 59 13 6,  67 13 6))");
        ValueGeometry geomC = ValueGeometry
                .get("POLYGON ((67 13 6, 67 18 5, 59 18 4, 59 13 5,  67 13 6))");
        assertEquals(geomA.hashCode(), geomB.hashCode());
        assertFalse(geomA.hashCode() == geomC.hashCode());
    }

    private void testSpatialValues() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(URL);
        Statement stat = conn.createStatement();

        stat.execute("create memory table test" +
                "(id int primary key, polygon geometry)");
        stat.execute("insert into test values(1, " +
                "'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
        ResultSet rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("POLYGON ((1 1, 1 2, 2 2, 1 1))", rs.getString(2));
        GeometryFactory f = new GeometryFactory();
        Polygon polygon = f.createPolygon(new Coordinate[] {
                new Coordinate(1, 1),
                new Coordinate(1, 2),
                new Coordinate(2, 2),
                new Coordinate(1, 1) });
        assertTrue(polygon.equals(rs.getObject(2)));

        rs = stat.executeQuery("select * from test where polygon = " +
                "'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        stat.executeQuery("select * from test where polygon > " +
                "'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        stat.executeQuery("select * from test where polygon < " +
                "'POLYGON ((1 1, 1 2, 2 2, 1 1))'");

        stat.execute("drop table test");
        conn.close();
        deleteDb("spatial");
    }

    /**
     * Generate a random line string under the given bounding box.
     *
     * @param geometryRand the random generator
     * @param minX Bounding box min x
     * @param maxX Bounding box max x
     * @param minY Bounding box min y
     * @param maxY Bounding box max y
     * @param maxLength LineString maximum length
     * @return A segment within this bounding box
     */
    static Geometry getRandomGeometry(Random geometryRand,
            double minX, double maxX,
            double minY, double maxY, double maxLength) {
        GeometryFactory factory = new GeometryFactory();
        // Create the start point
        Coordinate start = new Coordinate(
                geometryRand.nextDouble() * (maxX - minX) + minX,
                geometryRand.nextDouble() * (maxY - minY) + minY);
        // Compute an angle
        double angle = geometryRand.nextDouble() * Math.PI * 2;
        // Compute length
        double length = geometryRand.nextDouble() * maxLength;
        // Compute end point
        Coordinate end = new Coordinate(
                start.x + Math.cos(angle) * length,
                start.y + Math.sin(angle) * length);
        return factory.createLineString(new Coordinate[] { start, end });
    }

    private void testOverlap() throws SQLException {
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            stat.execute("create memory table test" +
                    "(id int primary key, poly geometry)");
            stat.execute("insert into test values(1, " +
                    "'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
            stat.execute("insert into test values(2, " +
                    "'POLYGON ((3 1, 3 2, 4 2, 3 1))')");
            stat.execute("insert into test values(3, " +
                    "'POLYGON ((1 3, 1 4, 2 4, 1 3))')");

            ResultSet rs = stat.executeQuery(
                    "select * from test " +
                    "where poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            stat.execute("drop table test");
        }
    }
    private void testPersistentSpatialIndex() throws SQLException {
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            stat.execute("create table test" +
                    "(id int primary key, poly geometry)");
            stat.execute("insert into test values(1, " +
                    "'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
            stat.execute("insert into test values(2,null)");
            stat.execute("insert into test values(3, " +
                    "'POLYGON ((3 1, 3 2, 4 2, 3 1))')");
            stat.execute("insert into test values(4,null)");
            stat.execute("insert into test values(5, " +
                    "'POLYGON ((1 3, 1 4, 2 4, 1 3))')");
            stat.execute("create spatial index on test(poly)");

            ResultSet rs = stat.executeQuery(
                    "select * from test " +
                    "where poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            rs.close();

            // Test with multiple operator
            rs = stat.executeQuery(
                    "select * from test " +
                    "where poly && 'POINT (1.5 1.5)'::Geometry " +
                    "AND poly && 'POINT (1.7 1.75)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            rs.close();
        }

        if (config.memory) {
            return;
        }

        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(
                    "select * from test " +
                    "where poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            stat.execute("drop table test");
        }
    }

    private void testNotOverlap() throws SQLException {
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            stat.execute("create memory table test" +
                    "(id int primary key, poly geometry)");
            stat.execute("insert into test values(1, " +
                    "'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
            stat.execute("insert into test values(2,null)");
            stat.execute("insert into test values(3, " +
                    "'POLYGON ((3 1, 3 2, 4 2, 3 1))')");
            stat.execute("insert into test values(4,null)");
            stat.execute("insert into test values(5, " +
                    "'POLYGON ((1 3, 1 4, 2 4, 1 3))')");

            ResultSet rs = stat.executeQuery(
                    "select * from test " +
                    "where NOT poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("id"));
            assertFalse(rs.next());
            stat.execute("drop table test");
        }
    }

    private static void createTestTable(Statement stat)  throws SQLException {
        stat.execute("create table area(idArea int primary key, the_geom geometry)");
        stat.execute("create spatial index on area(the_geom)");
        stat.execute("insert into area values(1, " +
                "'POLYGON ((-10 109, 90 109, 90 9, -10 9, -10 109))')");
        stat.execute("insert into area values(2, " +
                "'POLYGON ((90 109, 190 109, 190 9, 90 9, 90 109))')");
        stat.execute("insert into area values(3, " +
                "'POLYGON ((190 109, 290 109, 290 9, 190 9, 190 109))')");
        stat.execute("insert into area values(4, " +
                "'POLYGON ((-10 9, 90 9, 90 -91, -10 -91, -10 9))')");
        stat.execute("insert into area values(5, " +
                "'POLYGON ((90 9, 190 9, 190 -91, 90 -91, 90 9))')");
        stat.execute("insert into area values(6, " +
                "'POLYGON ((190 9, 290 9, 290 -91, 190 -91, 190 9))')");
        stat.execute("insert into area values(7,null)");
        stat.execute("insert into area values(8,null)");


        stat.execute("create table roads(idRoad int primary key, the_geom geometry)");
        stat.execute("create spatial index on roads(the_geom)");
        stat.execute("insert into roads values(1, " +
                "'LINESTRING (27.65595463138 -16.728733459357244, " +
                "47.61814744801515 40.435727788279806)')");
        stat.execute("insert into roads values(2, " +
                "'LINESTRING (17.674858223062415 55.861058601134246, " +
                "55.78449905482046 76.73062381852554)')");
        stat.execute("insert into roads values(3, " +
                "'LINESTRING (68.48771266540646 67.65689981096412, " +
                "108.4120982986768 88.52646502835542)')");
        stat.execute("insert into roads values(4, " +
                "'LINESTRING (177.3724007561437 18.65879017013235, " +
                "196.4272211720227 -16.728733459357244)')");
        stat.execute("insert into roads values(5, " +
                "'LINESTRING (106.5973534971645 -12.191871455576518, " +
                "143.79962192816637 30.454631379962223)')");
        stat.execute("insert into roads values(6, " +
                "'LINESTRING (144.70699432892252 55.861058601134246, " +
                "150.1512287334594 83.9896030245747)')");
        stat.execute("insert into roads values(7, " +
                "'LINESTRING (60.321361058601155 -13.099243856332663, " +
                "149.24385633270325 5.955576559546344)')");
        stat.execute("insert into roads values(8, null)");
        stat.execute("insert into roads values(9, null)");

    }

    private void testSpatialIndexQueryMultipleTable() throws SQLException {
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            createTestTable(stat);
            testRoadAndArea(stat);
        }
        deleteDb("spatial");
    }
    private void testRoadAndArea(Statement stat) throws SQLException {
        ResultSet rs = stat.executeQuery(
                "select idArea, COUNT(idRoad) roadCount " +
                "from area, roads " +
                "where area.the_geom && roads.the_geom " +
                "GROUP BY idArea ORDER BY idArea");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("idArea"));
        assertEquals(3, rs.getInt("roadCount"));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("idArea"));
        assertEquals(4, rs.getInt("roadCount"));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("idArea"));
        assertEquals(1, rs.getInt("roadCount"));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt("idArea"));
        assertEquals(2, rs.getInt("roadCount"));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt("idArea"));
        assertEquals(3, rs.getInt("roadCount"));
        assertTrue(rs.next());
        assertEquals(6, rs.getInt("idArea"));
        assertEquals(1, rs.getInt("roadCount"));
        assertFalse(rs.next());
        rs.close();
    }
    private void testIndexTransaction() throws SQLException {
        // Check session management in index
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            conn.setAutoCommit(false);
            Statement stat = conn.createStatement();
            createTestTable(stat);
            Savepoint sp = conn.setSavepoint();
            // Remove a row but do not commit
            stat.execute("delete from roads where idRoad=9");
            stat.execute("delete from roads where idRoad=7");
            // Check if index is updated
            ResultSet rs = stat.executeQuery(
                    "select idArea, COUNT(idRoad) roadCount " +
                    "from area, roads " +
                    "where area.the_geom && roads.the_geom " +
                    "GROUP BY idArea ORDER BY idArea");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("idArea"));
            assertEquals(3, rs.getInt("roadCount"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("idArea"));
            assertEquals(4, rs.getInt("roadCount"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("idArea"));
            assertEquals(1, rs.getInt("roadCount"));
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("idArea"));
            assertEquals(1, rs.getInt("roadCount"));
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("idArea"));
            assertEquals(2, rs.getInt("roadCount"));
            assertTrue(rs.next());
            assertEquals(6, rs.getInt("idArea"));
            assertEquals(1, rs.getInt("roadCount"));
            assertFalse(rs.next());
            rs.close();
            conn.rollback(sp);
            // Check if the index is restored
            testRoadAndArea(stat);
        }
    }

    /**
     * Test the in the in-memory spatial index
     */
    private void testMemorySpatialIndex() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(URL);
        Statement stat = conn.createStatement();

        stat.execute("create memory table test(id int primary key, polygon geometry)");
        stat.execute("create spatial index idx_test_polygon on test(polygon)");
        stat.execute("insert into test values(1, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
        stat.execute("insert into test values(2, null)");
        ResultSet rs;

        // an query that can not possibly return a result
        rs = stat.executeQuery("select * from test " +
                "where polygon && 'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry " +
                "and polygon && 'POLYGON ((10 10, 10 20, 20 20, 10 10))'::Geometry");
        assertFalse(rs.next());

        rs = stat.executeQuery(
                "explain select * from test " +
                "where polygon && 'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");
        rs.next();
        if (config.mvStore) {
            assertContains(rs.getString(1), "/* PUBLIC.IDX_TEST_POLYGON: POLYGON &&");
        }

        // TODO equality should probably also use the spatial index
        // rs = stat.executeQuery("explain select * from test " +
        //         "where polygon = 'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        // rs.next();
        // assertContains(rs.getString(1),
        //         "/* PUBLIC.IDX_TEST_POLYGON: POLYGON =");

        // these queries actually have no meaning in the context of a spatial
        // index, but
        // check them anyhow
        stat.executeQuery("select * from test where polygon > " +
                "'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");
        stat.executeQuery("select * from test where polygon < " +
                "'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");

        rs = stat.executeQuery(
                "select * from test " +
                "where intersects(polygon, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
        assertTrue(rs.next());

        rs = stat.executeQuery(
                "select * from test " +
                "where intersects(polygon, 'POINT (1 1)')");
        assertTrue(rs.next());

        rs = stat.executeQuery(
                "select * from test " +
                "where intersects(polygon, 'POINT (0 0)')");
        assertFalse(rs.next());

        stat.execute("drop table test");
        conn.close();
        deleteDb("spatial");
    }

    /**
     * Test java alias with Geometry type.
     */
    private void testJavaAlias() throws SQLException {
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            stat.execute("CREATE ALIAS T_GEOM_FROM_TEXT FOR \"" +
                    TestSpatial.class.getName() + ".geomFromText\"");
            stat.execute("create table test(id int primary key " +
                    "auto_increment, the_geom geometry)");
            stat.execute("insert into test(the_geom) values(" +
                    "T_GEOM_FROM_TEXT('POLYGON ((" +
                    "62 48, 84 48, 84 42, 56 34, 62 48))',1488))");
            stat.execute("DROP ALIAS T_GEOM_FROM_TEXT");
            ResultSet rs = stat.executeQuery("select the_geom from test");
            assertTrue(rs.next());
            assertEquals("POLYGON ((62 48, 84 48, 84 42, 56 34, 62 48))",
                    rs.getObject(1).toString());
        }
        deleteDb("spatial");
    }

    /**
     * Test java alias with Geometry type.
     */
    private void testJavaAliasTableFunction() throws SQLException {
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            stat.execute("CREATE ALIAS T_RANDOM_GEOM_TABLE FOR \"" +
                    TestSpatial.class.getName() + ".getRandomGeometryTable\"");
            stat.execute(
                    "create table test as " +
                    "select * from T_RANDOM_GEOM_TABLE(42,20,-100,100,-100,100,4)");
            stat.execute("DROP ALIAS T_RANDOM_GEOM_TABLE");
            ResultSet rs = stat.executeQuery("select count(*) from test");
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1));
        }
        deleteDb("spatial");
    }

    /**
     * Generate a result set with random geometry data.
     * Used as an ALIAS function.
     *
     * @param seed the random seed
     * @param rowCount the number of rows
     * @param minX the smallest x
     * @param maxX the largest x
     * @param minY the smallest y
     * @param maxY the largest y
     * @param maxLength the maximum length
     * @return a result set
     */
    public static ResultSet getRandomGeometryTable(
            final long seed, final long rowCount,
            final double minX, final double maxX,
            final double minY, final double maxY, final double maxLength) {

        SimpleResultSet rs = new SimpleResultSet(new SimpleRowSource() {

            private final Random random = new Random(seed);
            private int currentRow;

            @Override
            public Object[] readRow() throws SQLException {
                if (currentRow++ < rowCount) {
                    return new Object[] {
                            getRandomGeometry(random,
                                    minX, maxX, minY, maxY, maxLength) };
                }
                return null;
            }

            @Override
            public void close() {
                // nothing to do
            }

            @Override
            public void reset() throws SQLException {
                random.setSeed(seed);
            }
        });
        rs.addColumn("the_geom", Types.OTHER, "GEOMETRY", Integer.MAX_VALUE, 0);
        return rs;
    }

    /**
     * Convert the text to a geometry object.
     *
     * @param text the geometry as a Well Known Text
     * @param srid the projection id
     * @return Geometry object
     */
    public static Geometry geomFromText(String text, int srid) throws SQLException {
        WKTReader wktReader = new WKTReader();
        try {
            Geometry geom = wktReader.read(text);
            geom.setSRID(srid);
            return geom;
        } catch (ParseException ex) {
            throw new SQLException(ex);
        }
    }

    private void testGeometryDataType() {
        GeometryFactory geometryFactory = new GeometryFactory();
        Geometry geometry = geometryFactory.createPoint(new Coordinate(0, 0));
        assertEquals(Value.GEOMETRY, DataType.getTypeFromClass(geometry.getClass()));
    }

    /**
     * Test serialization of Z and SRID values.
     */
    private void testWKB() {
        ValueGeometry geom3d = ValueGeometry.get(
                "POLYGON ((67 13 6, 67 18 5, 59 18 4, 59 13 6,  67 13 6))", 27572);
        ValueGeometry copy = ValueGeometry.get(geom3d.getBytes());
        assertEquals(6, copy.getGeometry().getCoordinates()[0].z);
        assertEquals(5, copy.getGeometry().getCoordinates()[1].z);
        assertEquals(4, copy.getGeometry().getCoordinates()[2].z);
        // Test SRID
        copy = ValueGeometry.get(geom3d.getBytes());
        assertEquals(27572, copy.getGeometry().getSRID());
    }

    /**
     * Test conversion of Geometry object into Object
     */
    private void testValueConversion() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(URL);
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS OBJ_STRING FOR \"" +
                TestSpatial.class.getName() +
                ".getObjectString\"");
        ResultSet rs = stat.executeQuery(
                "select OBJ_STRING('POINT( 15 25 )'::geometry)");
        assertTrue(rs.next());
        assertEquals("POINT (15 25)", rs.getString(1));
        conn.close();
        deleteDb("spatial");
    }

    /**
     * Get the toString value of the object.
     *
     * @param object the object
     * @return the string representation
     */
    public static String getObjectString(Object object) {
        return object.toString();
    }

    /**
     * Test equality method on ValueGeometry
     */
    private void testEquals() {
        // 3d equality test
        ValueGeometry geom3d = ValueGeometry.get(
                "POLYGON ((67 13 6, 67 18 5, 59 18 4, 59 13 6,  67 13 6))");
        ValueGeometry geom2d = ValueGeometry.get(
                "POLYGON ((67 13, 67 18, 59 18, 59 13,  67 13))");
        assertFalse(geom3d.equals(geom2d));
        // SRID equality test
        GeometryFactory geometryFactory = new GeometryFactory();
        Geometry geometry = geometryFactory.createPoint(new Coordinate(0, 0));
        geometry.setSRID(27572);
        ValueGeometry valueGeometry =
                ValueGeometry.getFromGeometry(geometry);
        Geometry geometry2 = geometryFactory.createPoint(new Coordinate(0, 0));
        geometry2.setSRID(5326);
        ValueGeometry valueGeometry2 =
                ValueGeometry.getFromGeometry(geometry2);
        assertFalse(valueGeometry.equals(valueGeometry2));
        // Check illegal geometry (no WKB representation)
        try {
            ValueGeometry.get("POINT EMPTY");
            fail("expected this to throw IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected
        }
    }

    /**
     * Check that geometry column type is kept with a table function
     */
    private void testTableFunctionGeometry() throws SQLException {
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            stat.execute("CREATE ALIAS POINT_TABLE FOR \"" +
                    TestSpatial.class.getName() + ".pointTable\"");
            stat.execute("create table test as select * from point_table(1, 1)");
            // Read column type
            ResultSet columnMeta = conn.getMetaData().
                    getColumns(null, null, "TEST", "THE_GEOM");
            assertTrue(columnMeta.next());
            assertEquals("geometry",
                    columnMeta.getString("TYPE_NAME").toLowerCase());
            assertFalse(columnMeta.next());
        }
        deleteDb("spatial");
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param x the x position of the point
     * @param y the y position of the point
     * @return a result set with this point
     */
    public static ResultSet pointTable(double x, double y) {
        GeometryFactory factory = new GeometryFactory();
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("THE_GEOM", Types.JAVA_OBJECT, "GEOMETRY", 0, 0);
        rs.addRow(factory.createPoint(new Coordinate(x, y)));
        return rs;
    }

    private void testAggregateWithGeometry() throws SQLException {
        deleteDb("spatialIndex");
        try (Connection conn = getConnection("spatialIndex")) {
            Statement st = conn.createStatement();
            st.execute("CREATE AGGREGATE TABLE_ENVELOPE FOR \""+
                    TableEnvelope.class.getName()+"\"");
            st.execute("CREATE TABLE test(the_geom GEOMETRY)");
            st.execute("INSERT INTO test VALUES ('POINT(1 1)'), (null), (null), ('POINT(10 5)')");
            ResultSet rs = st.executeQuery("select TABLE_ENVELOPE(the_geom) from test");
            assertEquals("geometry", rs.getMetaData().
                    getColumnTypeName(1).toLowerCase());
            assertTrue(rs.next());
            assertTrue(rs.getObject(1) instanceof Geometry);
            assertTrue(new Envelope(1, 10, 1, 5).equals(
                    ((Geometry) rs.getObject(1)).getEnvelopeInternal()));
            assertFalse(rs.next());
        }
        deleteDb("spatialIndex");
    }

    /**
     * An aggregate function that calculates the envelope.
     */
    public static class TableEnvelope implements Aggregate {
        private Envelope tableEnvelope;

        @Override
        public int getInternalType(int[] inputTypes) throws SQLException {
            for (int inputType : inputTypes) {
                if (inputType != Value.GEOMETRY) {
                    throw new SQLException("TableEnvelope accept only Geometry argument");
                }
            }
            return Value.GEOMETRY;
        }

        @Override
        public void init(Connection conn) throws SQLException {
            tableEnvelope = null;
        }

        @Override
        public void add(Object value) throws SQLException {
            if (value instanceof Geometry) {
                if (tableEnvelope == null) {
                    tableEnvelope = ((Geometry) value).getEnvelopeInternal();
                } else {
                    tableEnvelope.expandToInclude(((Geometry) value).getEnvelopeInternal());
                }
            }
        }

        @Override
        public Object getResult() throws SQLException {
            return new GeometryFactory().toGeometry(tableEnvelope);
        }
    }

    private void testTableViewSpatialPredicate() throws SQLException {
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            stat.execute("drop table if exists test");
            stat.execute("drop view if exists test_view");
            stat.execute("create table test(id int primary key, poly geometry)");
            stat.execute("insert into test values(1, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
            stat.execute("insert into test values(4, null)");
            stat.execute("insert into test values(2, 'POLYGON ((3 1, 3 2, 4 2, 3 1))')");
            stat.execute("insert into test values(3, 'POLYGON ((1 3, 1 4, 2 4, 1 3))')");
            stat.execute("create view test_view as select * from test");

            //Check result with view
            ResultSet rs;
            rs = stat.executeQuery(
                    "select * from test where poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());

            rs = stat.executeQuery(
                    "select * from test_view where poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            rs.close();
        }
        deleteDb("spatial");
    }

    /**
     * Check ValueGeometry conversion into SQL script
     */
    private void testValueGeometryScript() throws SQLException {
        ValueGeometry valueGeometry = ValueGeometry.get("POINT(1 1 5)");
        try (Connection conn = getConnection(URL)) {
            ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT " + valueGeometry.getSQL());
            assertTrue(rs.next());
            Object obj = rs.getObject(1);
            ValueGeometry g = ValueGeometry.getFromGeometry(obj);
            assertTrue("got: " + g + " exp: " + valueGeometry, valueGeometry.equals(g));
        }
    }

    /**
     * If the user mutate the geometry of the object, the object cache must not
     * be updated.
     */
    private void testInPlaceUpdate() throws SQLException {
        try (Connection conn = getConnection(URL)) {
            ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT 'POINT(1 1)'::geometry");
            assertTrue(rs.next());
            // Mutate the geometry
            ((Geometry) rs.getObject(1)).apply(new AffineTransformation(1, 0,
                    1, 1, 0, 1));
            rs.close();
            rs = conn.createStatement().executeQuery(
                    "SELECT 'POINT(1 1)'::geometry");
            assertTrue(rs.next());
            // Check if the geometry is the one requested
            assertEquals(1, ((Point) rs.getObject(1)).getX());
            assertEquals(1, ((Point) rs.getObject(1)).getY());
            rs.close();
        }
    }

    private void testScanIndexOnNonSpatialQuery() throws SQLException {
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            stat.execute("drop table if exists test");
            stat.execute("create table test(id serial primary key, " +
                    "value double, the_geom geometry)");
            stat.execute("create spatial index spatial on test(the_geom)");
            ResultSet rs = stat.executeQuery("explain select * from test where _ROWID_ = 5");
            assertTrue(rs.next());
            assertFalse(rs.getString(1).contains("/* PUBLIC.SPATIAL: _ROWID_ = " +
                    "5 */"));
        }
        deleteDb("spatial");
    }

    private void testStoreCorruption() throws SQLException {
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            stat.execute("drop table if exists pt_cloud;\n" +
                    "CREATE TABLE PT_CLOUD AS " +
                    " SELECT CONCAT('POINT(',A.X,' ',B.X,')')::geometry the_geom from" +
                    " system_range(1e6,1e6+10) A,system_range(6e6,6e6+10) B;\n" +
                    "create spatial index pt_index on pt_cloud(the_geom);");
            // Wait some time
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                throw new SQLException(ex);
            }
            stat.execute("drop table if exists pt_cloud;\n" +
                    "CREATE TABLE PT_CLOUD AS " +
                    " SELECT CONCAT('POINT(',A.X,' ',B.X,')')::geometry the_geom from" +
                    " system_range(1e6,1e6+50) A,system_range(6e6,6e6+50) B;\n" +
                    "create spatial index pt_index on pt_cloud(the_geom);\n" +
                    "shutdown compact;");
        }
        deleteDb("spatial");
    }

    private void testExplainSpatialIndexWithPk() throws SQLException {
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            stat.execute("drop table if exists pt_cloud;");
            stat.execute("CREATE TABLE PT_CLOUD(id serial, the_geom geometry) AS " +
                "SELECT null, CONCAT('POINT(',A.X,' ',B.X,')')::geometry the_geom " +
                "from system_range(0,120) A,system_range(0,10) B;");
            stat.execute("create spatial index on pt_cloud(the_geom);");
            try (ResultSet rs = stat.executeQuery(
                    "explain select * from  PT_CLOUD " +
                            "where the_geom && 'POINT(1 1)'")) {
                assertTrue(rs.next());
                assertFalse("H2 should use spatial index got this explain:\n" +
                        rs.getString(1), rs.getString(1).contains("tableScan"));
            }
        }
        deleteDb("spatial");
    }

    private void testNullableGeometry() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(URL);
        Statement stat = conn.createStatement();

        stat.execute("create memory table test"
                + "(id int primary key, the_geom geometry)");
        stat.execute("create spatial index on test(the_geom)");
        stat.execute("insert into test values(1, null)");
        stat.execute("insert into test values(2, null)");
        stat.execute("delete from test where the_geom is null");
        stat.execute("insert into test values(1, null)");
        stat.execute("insert into test values(2, null)");
        stat.execute("insert into test values(3, " +
                "'POLYGON ((1000 2000, 1000 3000, 2000 3000, 1000 2000))')");
        stat.execute("insert into test values(4, null)");
        stat.execute("insert into test values(5, null)");
        stat.execute("insert into test values(6, " +
                "'POLYGON ((1000 3000, 1000 4000, 2000 4000, 1000 3000))')");

        ResultSet rs = stat.executeQuery("select * from test");
        int count = 0;
        while (rs.next()) {
            count++;
            int id = rs.getInt(1);
            if (id == 3 || id == 6) {
                assertTrue(rs.getObject(2) != null);
            } else {
                assertNull(rs.getObject(2));
            }
        }
        assertEquals(6, count);

        rs = stat.executeQuery("select * from test where the_geom is null");
        count = 0;
        while (rs.next()) {
            count++;
            assertNull(rs.getObject(2));
        }
        assertEquals(4, count);

        rs = stat.executeQuery("select * from test where the_geom is not null");
        count = 0;
        while (rs.next()) {
            count++;
            assertTrue(rs.getObject(2) != null);
        }
        assertEquals(2, count);

        rs = stat.executeQuery(
                "select * from test " +
                "where intersects(the_geom, " +
                "'POLYGON ((1000 1000, 1000 2000, 2000 2000, 1000 1000))')");

        conn.close();
        if (!config.memory) {
            conn = getConnection(URL);
            stat = conn.createStatement();
            rs = stat.executeQuery("select * from test");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertNull(rs.getObject(2));
            conn.close();
        }
        deleteDb("spatial");
    }

    private void testNullableGeometryDelete() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(URL);
        Statement stat = conn.createStatement();
        stat.execute("create memory table test"
                + "(id int primary key, the_geom geometry)");
        stat.execute("create spatial index on test(the_geom)");
        stat.execute("insert into test values(1, null)");
        stat.execute("insert into test values(2, null)");
        stat.execute("insert into test values(3, null)");
        ResultSet rs = stat.executeQuery("select * from test order by id");
        while (rs.next()) {
            assertNull(rs.getObject(2));
        }
        stat.execute("delete from test where id = 1");
        stat.execute("delete from test where id = 2");
        stat.execute("delete from test where id = 3");
        stat.execute("insert into test values(4, null)");
        stat.execute("insert into test values(5, null)");
        stat.execute("insert into test values(6, null)");
        stat.execute("delete from test where id = 4");
        stat.execute("delete from test where id = 5");
        stat.execute("delete from test where id = 6");
        conn.close();
        deleteDb("spatial");
    }

    private void testNullableGeometryInsert() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(URL);
        Statement stat = conn.createStatement();
        stat.execute("create memory table test"
                + "(id identity, the_geom geometry)");
        stat.execute("create spatial index on test(the_geom)");
        for (int i = 0; i < 1000; i++) {
            stat.execute("insert into test values(null, null)");
        }
        ResultSet rs = stat.executeQuery("select * from test");
        while (rs.next()) {
            assertNull(rs.getObject(2));
        }
        conn.close();
        deleteDb("spatial");
    }

    private void testNullableGeometryUpdate() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(URL);
        Statement stat = conn.createStatement();
        stat.execute("create memory table test"
                + "(id int primary key, the_geom geometry, description varchar2(32))");
        stat.execute("create spatial index on test(the_geom)");
        for (int i = 0; i < 1000; i++) {
            stat.execute("insert into test values("+ (i + 1) +", null, null)");
        }
        ResultSet rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertNull(rs.getObject(2));
        stat.execute("update test set description='DESCRIPTION' where id = 1");
        stat.execute("update test set description='DESCRIPTION' where id = 2");
        stat.execute("update test set description='DESCRIPTION' where id = 3");
        conn.close();
        deleteDb("spatial");
    }

    private void testIndexUpdateNullGeometry() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(URL);
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists DUMMY_11;");
        stat.execute("CREATE TABLE PUBLIC.DUMMY_11 (fid serial,  GEOM GEOMETRY);");
        stat.execute("CREATE SPATIAL INDEX PUBLIC_DUMMY_11_SPATIAL_INDEX on" +
                " PUBLIC.DUMMY_11(GEOM);");
        stat.execute("insert into PUBLIC.DUMMY_11(geom) values(null);");
        stat.execute("update PUBLIC.DUMMY_11 set geom =" +
                " 'POLYGON ((1 1, 5 1, 5 5, 1 5, 1 1))';");
        ResultSet rs = stat.executeQuery("select fid, GEOM from DUMMY_11 " +
                "where  GEOM && " +
                "'POLYGON" +
                "((1 1,5 1,5 5,1 5,1 1))';");
        try {
            assertTrue(rs.next());
            assertEquals("POLYGON ((1 1, 5 1, 5 5, 1 5, 1 1))", rs.getString(2));
        } finally {
            rs.close();
        }
        // Update again the geometry elsewhere
        stat.execute("update PUBLIC.DUMMY_11 set geom =" +
                " 'POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10))';");

        rs = stat.executeQuery("select fid, GEOM from DUMMY_11 " +
                "where  GEOM && " +
                "'POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10))';");
        try {
            assertTrue(rs.next());
            assertEquals("POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10))", rs.getString(2));
        } finally {
            rs.close();
        }
        conn.close();
        deleteDb("spatial");
    }

    private void testInsertNull() throws SQLException {
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            stat.execute("\n" +
                "drop table if exists PUBLIC.DUMMY_12;\n" +
                "CREATE TABLE PUBLIC.DUMMY_12 (\n" +
                "    \"fid\" serial,\n" +
                "    Z_ID INTEGER,\n" +
                "    GEOM GEOMETRY,\n" +
                "    CONSTRAINT CONSTRAINT_DUMMY_12 PRIMARY KEY (\"fid\")\n" +
                ");\n" +
                "CREATE INDEX PRIMARY_KEY_DUMMY_12 ON PUBLIC.DUMMY_12 (\"fid\");\n" +
                "CREATE spatial INDEX PUBLIC_DUMMY_12_SPATIAL_INDEX_ ON PUBLIC.DUMMY_12 (GEOM);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (123,3125163,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (124,3125164,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (125,3125173,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (126,3125174,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (127,3125175,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (128,3125176,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (129,3125177,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (130,3125178,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (131,3125179,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (132,3125180,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (133,3125335,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (134,3125336,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (135,3125165,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (136,3125337,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (137,3125338,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (138,3125339,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (139,3125340,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (140,3125341,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (141,3125342,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (142,3125343,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (143,3125344,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (144,3125345,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (145,3125346,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (146,3125166,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (147,3125347,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (148,3125348,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (149,3125349,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (150,3125350,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (151,3125351,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (152,3125352,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (153,3125353,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (154,3125354,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (155,3125355,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (156,3125356,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (157,3125167,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (158,3125357,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (159,3125358,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (160,3125359,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (161,3125360,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (162,3125361,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (163,3125362,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (164,3125363,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (165,3125364,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (166,3125365,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (167,3125366,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (168,3125168,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (169,3125367,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (170,3125368,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (171,3125369,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (172,3125370,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (173,3125169,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (174,3125170,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (175,3125171,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (176,3125172,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (177,-2,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (178,-1,NULL);\n" +
                "INSERT INTO PUBLIC.DUMMY_12 (\"fid\",Z_ID,GEOM) VALUES (179," +
                "-1,NULL);");
            try (ResultSet rs = stat.executeQuery("select * from DUMMY_12")) {
                assertTrue(rs.next());
            }
        }
        deleteDb("spatial");
    }

    private void testSpatialIndexWithOrder() throws SQLException {
        deleteDb("spatial");
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            stat.execute("DROP TABLE IF EXISTS BUILDINGS;" +
                    "CREATE TABLE BUILDINGS (PK serial, THE_GEOM geometry);" +
                    "insert into buildings(the_geom) SELECT 'POINT(1 1)" +
                    "'::geometry from SYSTEM_RANGE(1,10000);\n" +
                    "CREATE SPATIAL INDEX ON PUBLIC.BUILDINGS(THE_GEOM);\n");

            try (ResultSet rs = stat.executeQuery("EXPLAIN SELECT * FROM " +
                    "BUILDINGS ORDER BY PK LIMIT 51;")) {
                assertTrue(rs.next());
                assertTrue(rs.getString(1).contains("PRIMARY_KEY"));
            }
        }
        deleteDb("spatial");
    }
}
