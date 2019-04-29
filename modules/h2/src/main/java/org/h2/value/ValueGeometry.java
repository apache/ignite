/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import static org.h2.util.geometry.EWKBUtils.EWKB_SRID;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import org.h2.api.ErrorCode;
import org.h2.engine.Mode;
import org.h2.message.DbException;
import org.h2.util.Bits;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.util.geometry.EWKBUtils;
import org.h2.util.geometry.EWKTUtils;
import org.h2.util.geometry.GeometryUtils;
import org.h2.util.geometry.GeometryUtils.EnvelopeAndDimensionSystemTarget;
import org.h2.util.geometry.GeometryUtils.EnvelopeTarget;
import org.h2.util.geometry.JTSUtils;
import org.locationtech.jts.geom.Geometry;

/**
 * Implementation of the GEOMETRY data type.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class ValueGeometry extends Value {

    private static final double[] UNKNOWN_ENVELOPE = new double[0];

    /**
     * As conversion from/to WKB cost a significant amount of CPU cycles, WKB
     * are kept in ValueGeometry instance.
     *
     * We always calculate the WKB, because not all WKT values can be
     * represented in WKB, but since we persist it in WKB format, it has to be
     * valid in WKB
     */
    private final byte[] bytes;

    private final int hashCode;

    /**
     * Geometry type and dimension system in OGC geometry code format (type +
     * dimensionSystem * 1000).
     */
    private final int typeAndDimensionSystem;

    /**
     * Spatial reference system identifier.
     */
    private final int srid;

    /**
     * The envelope of the value. Calculated only on request.
     */
    private double[] envelope;

    /**
     * The value. Converted from WKB only on request as conversion from/to WKB
     * cost a significant amount of CPU cycles.
     */
    private Object geometry;

    /**
     * Create a new geometry object.
     *
     * @param bytes the EWKB bytes
     * @param envelope the envelope
     */
    private ValueGeometry(byte[] bytes, double[] envelope) {
        if (bytes.length < 9 || bytes[0] != 0) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, StringUtils.convertBytesToHex(bytes));
        }
        this.bytes = bytes;
        this.envelope = envelope;
        int t = Bits.readInt(bytes, 1);
        srid = (t & EWKB_SRID) != 0 ? Bits.readInt(bytes, 5) : 0;
        typeAndDimensionSystem = (t & 0xffff) % 1_000 + EWKBUtils.type2dimensionSystem(t) * 1_000;
        hashCode = Arrays.hashCode(bytes);
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param o the geometry object (of type
     *            org.locationtech.jts.geom.Geometry)
     * @return the value
     */
    public static ValueGeometry getFromGeometry(Object o) {
        try {
            EnvelopeAndDimensionSystemTarget target = new EnvelopeAndDimensionSystemTarget();
            Geometry g = (Geometry) o;
            JTSUtils.parseGeometry(g, target);
            return (ValueGeometry) Value.cache(new ValueGeometry( //
                    JTSUtils.geometry2ewkb(g, target.getDimensionSystem()), target.getEnvelope()));
        } catch (RuntimeException ex) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, String.valueOf(o));
        }
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param s the WKT or EWKT representation of the geometry
     * @return the value
     */
    public static ValueGeometry get(String s) {
        try {
            EnvelopeAndDimensionSystemTarget target = new EnvelopeAndDimensionSystemTarget();
            EWKTUtils.parseEWKT(s, target);
            return (ValueGeometry) Value.cache(new ValueGeometry( //
                    EWKTUtils.ewkt2ewkb(s, target.getDimensionSystem()), target.getEnvelope()));
        } catch (RuntimeException ex) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
        }
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param s the WKT representation of the geometry
     * @param srid the srid of the object
     * @return the value
     */
    public static ValueGeometry get(String s, int srid) {
        // This method is not used in H2, but preserved for H2GIS
        return get(srid == 0 ? s : "SRID=" + srid + ';' + s);
    }

    /**
     * Get or create a geometry value for the given internal EWKB representation.
     *
     * @param bytes the WKB representation of the geometry. May not be modified.
     * @return the value
     */
    public static ValueGeometry get(byte[] bytes) {
        return (ValueGeometry) Value.cache(new ValueGeometry(bytes, UNKNOWN_ENVELOPE));
    }

    /**
     * Get or create a geometry value for the given EWKB value.
     *
     * @param bytes the WKB representation of the geometry
     * @return the value
     */
    public static ValueGeometry getFromEWKB(byte[] bytes) {
        try {
            EnvelopeAndDimensionSystemTarget target = new EnvelopeAndDimensionSystemTarget();
            EWKBUtils.parseEWKB(bytes, target);
            return (ValueGeometry) Value.cache(new ValueGeometry( //
                    EWKBUtils.ewkb2ewkb(bytes, target.getDimensionSystem()), target.getEnvelope()));
        } catch (RuntimeException ex) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, StringUtils.convertBytesToHex(bytes));
        }
    }

    /**
     * Creates a geometry value for the given envelope.
     *
     * @param envelope envelope. May not be modified.
     * @return the value
     */
    public static Value fromEnvelope(double[] envelope) {
        return envelope != null
                ? Value.cache(new ValueGeometry(EWKBUtils.envelope2wkb(envelope), envelope))
                : ValueNull.INSTANCE;
    }

    /**
     * Get a copy of geometry object. Geometry object is mutable. The returned
     * object is therefore copied before returning.
     *
     * @return a copy of the geometry object
     */
    public Geometry getGeometry() {
        if (geometry == null) {
            try {
                geometry = JTSUtils.ewkb2geometry(bytes, getDimensionSystem());
            } catch (RuntimeException ex) {
                throw DbException.convert(ex);
            }
        }
        return ((Geometry) geometry).copy();
    }

    /**
     * Returns geometry type and dimension system in OGC geometry code format
     * (type + dimensionSystem * 1000).
     *
     * @return geometry type and dimension system
     */
    public int getTypeAndDimensionSystem() {
        return typeAndDimensionSystem;
    }

    /**
     * Returns geometry type.
     *
     * @return geometry type and dimension system
     */
    public int getGeometryType() {
        return typeAndDimensionSystem % 1_000;
    }

    /**
     * Return a minimal dimension system that can be used for this geometry.
     *
     * @return dimension system
     */
    public int getDimensionSystem() {
        return typeAndDimensionSystem / 1_000;
    }

    /**
     * Return a spatial reference system identifier.
     *
     * @return spatial reference system identifier
     */
    public int getSRID() {
        return srid;
    }

    /**
     * Return an envelope of this geometry. Do not modify the returned value.
     *
     * @return envelope of this geometry
     */
    public double[] getEnvelopeNoCopy() {
        if (envelope == UNKNOWN_ENVELOPE) {
            EnvelopeTarget target = new EnvelopeTarget();
            EWKBUtils.parseEWKB(bytes, target);
            envelope = target.getEnvelope();
        }
        return envelope;
    }

    /**
     * Test if this geometry envelope intersects with the other geometry
     * envelope.
     *
     * @param r the other geometry
     * @return true if the two overlap
     */
    public boolean intersectsBoundingBox(ValueGeometry r) {
        return GeometryUtils.intersects(getEnvelopeNoCopy(), r.getEnvelopeNoCopy());
    }

    /**
     * Get the union.
     *
     * @param r the other geometry
     * @return the union of this geometry envelope and another geometry envelope
     */
    public Value getEnvelopeUnion(ValueGeometry r) {
        return fromEnvelope(GeometryUtils.union(getEnvelopeNoCopy(), r.getEnvelopeNoCopy()));
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_GEOMETRY;
    }

    @Override
    public int getValueType() {
        return GEOMETRY;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        // Using bytes is faster than converting to EWKT.
        builder.append("X'");
        return StringUtils.convertBytesToHex(builder, getBytesNoCopy()).append("'::Geometry");
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode) {
        return Bits.compareNotNullUnsigned(bytes, ((ValueGeometry) v).bytes);
    }

    @Override
    public String getString() {
        return getEWKT();
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public Object getObject() {
        if (DataType.GEOMETRY_CLASS != null) {
            return getGeometry();
        }
        return getEWKT();
    }

    @Override
    public byte[] getBytes() {
        return Utils.cloneByteArray(bytes);
    }

    @Override
    public byte[] getBytesNoCopy() {
        return bytes;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setBytes(parameterIndex, bytes);
    }

    @Override
    public int getMemory() {
        return bytes.length * 20 + 24;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueGeometry && Arrays.equals(bytes, ((ValueGeometry) other).bytes);
    }

    /**
     * Get the value in Extended Well-Known Text format.
     *
     * @return the extended well-known text
     */
    public String getEWKT() {
        return EWKTUtils.ewkb2ewkt(bytes, getDimensionSystem());
    }

    /**
     * Get the value in extended Well-Known Binary format.
     *
     * @return the extended well-known binary
     */
    public byte[] getEWKB() {
        return bytes;
    }

    @Override
    protected Value convertTo(int targetType, Mode mode, Object column, ExtTypeInfo extTypeInfo) {
        if (targetType == Value.GEOMETRY) {
            return extTypeInfo != null ? extTypeInfo.cast(this) : this;
        } else if (targetType == Value.JAVA_OBJECT) {
            return this;
        }
        return super.convertTo(targetType, mode, column, null);
    }

}
