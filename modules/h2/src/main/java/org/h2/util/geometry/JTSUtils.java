/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.geometry;

import static org.h2.util.geometry.GeometryUtils.DIMENSION_SYSTEM_XYM;
import static org.h2.util.geometry.GeometryUtils.DIMENSION_SYSTEM_XYZ;
import static org.h2.util.geometry.GeometryUtils.GEOMETRY_COLLECTION;
import static org.h2.util.geometry.GeometryUtils.LINE_STRING;
import static org.h2.util.geometry.GeometryUtils.M;
import static org.h2.util.geometry.GeometryUtils.MULTI_LINE_STRING;
import static org.h2.util.geometry.GeometryUtils.MULTI_POINT;
import static org.h2.util.geometry.GeometryUtils.MULTI_POLYGON;
import static org.h2.util.geometry.GeometryUtils.POINT;
import static org.h2.util.geometry.GeometryUtils.POLYGON;
import static org.h2.util.geometry.GeometryUtils.X;
import static org.h2.util.geometry.GeometryUtils.Y;
import static org.h2.util.geometry.GeometryUtils.Z;
import static org.h2.util.geometry.GeometryUtils.checkFinite;
import static org.h2.util.geometry.GeometryUtils.toCanonicalDouble;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;

import org.h2.message.DbException;
import org.h2.util.geometry.EWKBUtils.EWKBTarget;
import org.h2.util.geometry.GeometryUtils.DimensionSystemTarget;
import org.h2.util.geometry.GeometryUtils.Target;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;

/**
 * Utilities for Geometry data type from JTS library.
 */
public final class JTSUtils {

    /**
     * {@code true} if M dimension is supported by used version of JTS,
     * {@code false} if M dimension is only partially supported (JTS 1.15).
     */
    public static final boolean M_IS_SUPPORTED;

    /**
     * create(int,int,int) method from CoordinateSequenceFactory, if it exists
     */
    static final Method CREATE;

    private static final Method GET_MEASURES;

    static {
        Method create, getMeasures;
        try {
            create = CoordinateSequenceFactory.class.getMethod("create", int.class, int.class, int.class);
            getMeasures = CoordinateSequence.class.getMethod("getMeasures");
        } catch (ReflectiveOperationException e) {
            create = null;
            getMeasures = null;
        }
        M_IS_SUPPORTED = create != null;
        CREATE = create;
        GET_MEASURES = getMeasures;
    }

    /**
     * Converter output target that creates a JTS Geometry.
     */
    public static final class GeometryTarget extends Target {

        private final int dimensionSystem;

        private GeometryFactory factory;

        private int type;

        private CoordinateSequence coordinates;

        private CoordinateSequence[] innerCoordinates;

        private int innerOffset;

        private Geometry[] subgeometries;

        /**
         * Creates a new instance of JTS Geometry target.
         *
         * @param dimensionSystem
         *            dimension system to use
         */
        public GeometryTarget(int dimensionSystem) {
            this.dimensionSystem = dimensionSystem;
        }

        private GeometryTarget(int dimensionSystem, GeometryFactory factory) {
            this.dimensionSystem = dimensionSystem;
            this.factory = factory;
        }

        @Override
        protected void init(int srid) {
            factory = new GeometryFactory(new PrecisionModel(), srid,
                    (dimensionSystem & DIMENSION_SYSTEM_XYM) != 0 ? PackedCoordinateSequenceFactory.DOUBLE_FACTORY
                            : CoordinateArraySequenceFactory.instance());
        }

        @Override
        protected void startPoint() {
            type = POINT;
            initCoordinates(1);
            innerOffset = -1;
        }

        @Override
        protected void startLineString(int numPoints) {
            type = LINE_STRING;
            initCoordinates(numPoints);
            innerOffset = -1;
        }

        @Override
        protected void startPolygon(int numInner, int numPoints) {
            type = POLYGON;
            initCoordinates(numPoints);
            innerCoordinates = new CoordinateSequence[numInner];
            innerOffset = -1;
        }

        @Override
        protected void startPolygonInner(int numInner) {
            innerCoordinates[++innerOffset] = createCoordinates(numInner);
        }

        @Override
        protected void startCollection(int type, int numItems) {
            this.type = type;
            switch (type) {
            case MULTI_POINT:
                subgeometries = new Point[numItems];
                break;
            case MULTI_LINE_STRING:
                subgeometries = new LineString[numItems];
                break;
            case MULTI_POLYGON:
                subgeometries = new Polygon[numItems];
                break;
            case GEOMETRY_COLLECTION:
                subgeometries = new Geometry[numItems];
                break;
            default:
                throw new IllegalArgumentException();
            }
        }

        @Override
        protected Target startCollectionItem(int index, int total) {
            return new GeometryTarget(dimensionSystem, factory);
        }

        @Override
        protected void endCollectionItem(Target target, int index, int total) {
            subgeometries[index] = ((GeometryTarget) target).getGeometry();
        }

        private void initCoordinates(int numPoints) {
            coordinates = createCoordinates(numPoints);
        }

        private CoordinateSequence createCoordinates(int numPoints) {
            if ((dimensionSystem & DIMENSION_SYSTEM_XYM) != 0) {
                if (M_IS_SUPPORTED) {
                    try {
                        return (CoordinateSequence) CREATE.invoke(factory.getCoordinateSequenceFactory(), numPoints, 4,
                                1);
                    } catch (ReflectiveOperationException e) {
                        throw DbException.convert(e);
                    }
                }
                return factory.getCoordinateSequenceFactory().create(numPoints, 4);
            } else {
                return factory.getCoordinateSequenceFactory().create(numPoints, 3);
            }
        }

        @Override
        protected void addCoordinate(double x, double y, double z, double m, int index, int total) {
            if (type == POINT && Double.isNaN(x) && Double.isNaN(y) && Double.isNaN(z) && Double.isNaN(m)) {
                this.coordinates = createCoordinates(0);
                return;
            }
            CoordinateSequence coordinates = innerOffset < 0 ? this.coordinates : innerCoordinates[innerOffset];
            coordinates.setOrdinate(index, X, checkFinite(x));
            coordinates.setOrdinate(index, Y, checkFinite(y));
            coordinates.setOrdinate(index, Z,
                    (dimensionSystem & DIMENSION_SYSTEM_XYZ) != 0 ? checkFinite(z) : Double.NaN);
            if ((dimensionSystem & DIMENSION_SYSTEM_XYM) != 0) {
                coordinates.setOrdinate(index, M, checkFinite(m));
            }
        }

        Geometry getGeometry() {
            switch (type) {
            case POINT:
                return new Point(coordinates, factory);
            case LINE_STRING:
                return new LineString(coordinates, factory);
            case POLYGON: {
                LinearRing shell = new LinearRing(coordinates, factory);
                int innerCount = innerCoordinates.length;
                LinearRing[] holes = new LinearRing[innerCount];
                for (int i = 0; i < innerCount; i++) {
                    holes[i] = new LinearRing(innerCoordinates[i], factory);
                }
                return new Polygon(shell, holes, factory);
            }
            case MULTI_POINT:
                return new MultiPoint((Point[]) subgeometries, factory);
            case MULTI_LINE_STRING:
                return new MultiLineString((LineString[]) subgeometries, factory);
            case MULTI_POLYGON:
                return new MultiPolygon((Polygon[]) subgeometries, factory);
            case GEOMETRY_COLLECTION:
                return new GeometryCollection(subgeometries, factory);
            default:
                throw new IllegalStateException();
            }
        }

    }

    /**
     * Converts EWKB to a JTS geometry object.
     *
     * @param ewkb
     *            source EWKB
     * @return JTS geometry object
     */
    public static Geometry ewkb2geometry(byte[] ewkb) {
        // Determine dimension system first
        DimensionSystemTarget dimensionTarget = new DimensionSystemTarget();
        EWKBUtils.parseEWKB(ewkb, dimensionTarget);
        // Generate a Geometry
        return ewkb2geometry(ewkb, dimensionTarget.getDimensionSystem());
    }

    /**
     * Converts EWKB to a JTS geometry object.
     *
     * @param ewkb
     *            source EWKB
     * @param dimensionSystem
     *            dimension system
     * @return JTS geometry object
     */
    public static Geometry ewkb2geometry(byte[] ewkb, int dimensionSystem) {
        GeometryTarget target = new GeometryTarget(dimensionSystem);
        EWKBUtils.parseEWKB(ewkb, target);
        return target.getGeometry();
    }

    /**
     * Converts Geometry to EWKB.
     *
     * @param geometry
     *            source geometry
     * @return EWKB representation
     */
    public static byte[] geometry2ewkb(Geometry geometry) {
        // Determine dimension system first
        DimensionSystemTarget dimensionTarget = new DimensionSystemTarget();
        parseGeometry(geometry, dimensionTarget);
        // Write an EWKB
        return geometry2ewkb(geometry, dimensionTarget.getDimensionSystem());
    }

    /**
     * Converts Geometry to EWKB.
     *
     * @param geometry
     *            source geometry
     * @param dimensionSystem
     *            dimension system
     * @return EWKB representation
     */
    public static byte[] geometry2ewkb(Geometry geometry, int dimensionSystem) {
        // Write an EWKB
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        EWKBTarget target = new EWKBTarget(output, dimensionSystem);
        parseGeometry(geometry, target);
        return output.toByteArray();
    }

    /**
     * Parses a JTS Geometry object.
     *
     * @param geometry
     *            geometry to parse
     * @param target
     *            output target
     */
    public static void parseGeometry(Geometry geometry, Target target) {
        parseGeometry(geometry, target, 0);
    }

    /**
     * Parses a JTS Geometry object.
     *
     * @param geometry
     *            geometry to parse
     * @param target
     *            output target
     * @param parentType
     *            type of parent geometry collection, or 0 for the root geometry
     */
    private static void parseGeometry(Geometry geometry, Target target, int parentType) {
        if (parentType == 0) {
            target.init(geometry.getSRID());
        }
        if (geometry instanceof Point) {
            if (parentType != 0 && parentType != MULTI_POINT && parentType != GEOMETRY_COLLECTION) {
                throw new IllegalArgumentException();
            }
            target.startPoint();
            Point p = (Point) geometry;
            if (p.isEmpty()) {
                target.addCoordinate(Double.NaN, Double.NaN, Double.NaN, Double.NaN, 0, 1);
            } else {
                CoordinateSequence sequence = p.getCoordinateSequence();
                addCoordinate(sequence, target, 0, 1, getMeasures(sequence));
            }
        } else if (geometry instanceof LineString) {
            if (parentType != 0 && parentType != MULTI_LINE_STRING && parentType != GEOMETRY_COLLECTION) {
                throw new IllegalArgumentException();
            }
            LineString ls = (LineString) geometry;
            CoordinateSequence cs = ls.getCoordinateSequence();
            int numPoints = cs.size();
            if (numPoints < 0 || numPoints == 1) {
                throw new IllegalArgumentException();
            }
            target.startLineString(numPoints);
            int measures = getMeasures(cs);
            for (int i = 0; i < numPoints; i++) {
                addCoordinate(cs, target, i, numPoints, measures);
            }
        } else if (geometry instanceof Polygon) {
            if (parentType != 0 && parentType != MULTI_POLYGON && parentType != GEOMETRY_COLLECTION) {
                throw new IllegalArgumentException();
            }
            Polygon p = (Polygon) geometry;
            int numInner = p.getNumInteriorRing();
            if (numInner < 0) {
                throw new IllegalArgumentException();
            }
            CoordinateSequence cs = p.getExteriorRing().getCoordinateSequence();
            int size = cs.size();
            // Size may be 0 (EMPTY) or 4+
            if (size < 0 || size >= 1 && size <= 3) {
                throw new IllegalArgumentException();
            }
            if (size == 0 && numInner > 0) {
                throw new IllegalArgumentException();
            }
            target.startPolygon(numInner, size);
            if (size > 0) {
                int measures = getMeasures(cs);
                addRing(cs, target, size, measures);
                for (int i = 0; i < numInner; i++) {
                    cs = p.getInteriorRingN(i).getCoordinateSequence();
                    size = cs.size();
                    // Size may be 0 (EMPTY) or 4+
                    if (size < 0 || size >= 1 && size <= 3) {
                        throw new IllegalArgumentException();
                    }
                    target.startPolygonInner(size);
                    addRing(cs, target, size, measures);
                }
                target.endNonEmptyPolygon();
            }
        } else if (geometry instanceof GeometryCollection) {
            if (parentType != 0 && parentType != GEOMETRY_COLLECTION) {
                throw new IllegalArgumentException();
            }
            GeometryCollection gc = (GeometryCollection) geometry;
            int type;
            if (gc instanceof MultiPoint) {
                type = MULTI_POINT;
            } else if (gc instanceof MultiLineString) {
                type = MULTI_LINE_STRING;
            } else if (gc instanceof MultiPolygon) {
                type = MULTI_POLYGON;
            } else {
                type = GEOMETRY_COLLECTION;
            }
            int numItems = gc.getNumGeometries();
            if (numItems < 0) {
                throw new IllegalArgumentException();
            }
            target.startCollection(type, numItems);
            for (int i = 0; i < numItems; i++) {
                Target innerTarget = target.startCollectionItem(i, numItems);
                parseGeometry(gc.getGeometryN(i), innerTarget, type);
                target.endCollectionItem(innerTarget, i, numItems);
            }
            target.endCollection(type);
        } else {
            throw new IllegalArgumentException();
        }

    }

    private static void addRing(CoordinateSequence sequence, Target target, int size, int measures) {
        // 0 or 4+ are valid
        if (size >= 4) {
            double startX = toCanonicalDouble(sequence.getX(0)), startY = toCanonicalDouble(sequence.getY(0));
            addCoordinate(sequence, target, 0, size, startX, startY, measures);
            for (int i = 1; i < size - 1; i++) {
                addCoordinate(sequence, target, i, size, measures);
            }
            double endX = toCanonicalDouble(sequence.getX(size - 1)), //
                    endY = toCanonicalDouble(sequence.getY(size - 1));
            /*
             * TODO OGC 06-103r4 determines points as equal if they have the
             * same X and Y coordinates. Should we check Z and M here too?
             */
            if (startX != endX || startY != endY) {
                throw new IllegalArgumentException();
            }
            addCoordinate(sequence, target, size - 1, size, endX, endY, measures);
        }
    }

    private static void addCoordinate(CoordinateSequence sequence, Target target, int index, int total, int measures) {
        addCoordinate(sequence, target, index, total, toCanonicalDouble(sequence.getX(index)),
                toCanonicalDouble(sequence.getY(index)), measures);
    }

    private static void addCoordinate(CoordinateSequence sequence, Target target, int index, int total, double x,
            double y, int measures) {
        double m, z;
        int d = sequence.getDimension();
        if (M_IS_SUPPORTED) {
            d -= measures;
            z = d > 2 ? toCanonicalDouble(sequence.getOrdinate(index, Z)) : Double.NaN;
            m = measures >= 1 ? toCanonicalDouble(sequence.getOrdinate(index, d)) : Double.NaN;
        } else {
            z = d >= 3 ? toCanonicalDouble(sequence.getOrdinate(index, Z)) : Double.NaN;
            m = d >= 4 ? toCanonicalDouble(sequence.getOrdinate(index, M)) : Double.NaN;
        }
        target.addCoordinate(x, y, z, m, index, total);
    }

    private static int getMeasures(CoordinateSequence sequence) {
        int m;
        if (M_IS_SUPPORTED) {
            try {
                m = (int) GET_MEASURES.invoke(sequence);
            } catch (ReflectiveOperationException e) {
                throw DbException.convert(e);
            }
        } else {
            m = 0;
        }
        return m;
    }

    private JTSUtils() {
    }

}
