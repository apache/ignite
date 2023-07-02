/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.common;

import org.hawkore.ignite.lucene.IndexException;
import org.locationtech.spatial4j.distance.DistanceUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

/**
 * Class representing a geographical distance.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class GeoDistance implements Comparable<GeoDistance> {

    /** The quantitative distance value. */
    private final double value;

    /** The distance unit. */
    private final GeoDistanceUnit unit;

    /**
     * Builds a new {@link GeoDistance} defined by the specified quantitative value and distance unit.
     *
     * @param value The quantitative distance value.
     * @param unit The distance unit.
     */
    private GeoDistance(double value, GeoDistanceUnit unit) {
        this.value = value;
        this.unit = unit;
    }

    /**
     * Returns the numeric distance value in the specified unit.
     *
     * @param unit The distance unit to be used.
     * @return The numeric distance value in the specified unit.
     */
    public double getValue(GeoDistanceUnit unit) {
        return this.unit.getMetres() * value / unit.getMetres();
    }

    /**
     * Return the numeric distance value in degrees.
     *
     * @return the degrees
     */
    public double getDegrees() {
        double kms = getValue(GeoDistanceUnit.KILOMETRES);
        return DistanceUtils.dist2Degrees(kms, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    }

    /**
     * Returns the {@link GeoDistance} represented by the specified JSON {@code String}.
     *
     * @param json A {@code String} containing a JSON encoded {@link GeoDistance}.
     * @return The {@link GeoDistance} represented by the specified JSON {@code String}.
     */
    @JsonCreator
    public static GeoDistance parse(String json) {
        try {
            if (json.length()==0){
                return null;
            }
            String unit = null;
            for (GeoDistanceUnit geoDistanceUnit : GeoDistanceUnit.values()) {
                for (String name : geoDistanceUnit.getNames()) {
                    if (json.endsWith(name) && (unit == null || unit.length() < name.length())) {
                        unit = name;
                    }
                }
            }
            if (unit != null) {
                double value = Double.parseDouble(json.substring(0, json.indexOf(unit)));
                return new GeoDistance(value, GeoDistanceUnit.create(unit));
            }
            double value = Double.parseDouble(json);
            return new GeoDistance(value, GeoDistanceUnit.METRES);
        } catch (Exception e) {
            throw new IndexException(e, "Unparseable distance: {}", json);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(GeoDistance other) {
        return Double.valueOf(getValue(GeoDistanceUnit.MILLIMETRES))
                     .compareTo(other.getValue(GeoDistanceUnit.MILLIMETRES));
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("value", value).add("unit", unit).toString();
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GeoDistance that = (GeoDistance) o;
        return Double.compare(that.value, value) == 0 && unit == that.unit;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(value);
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + (unit != null ? unit.hashCode() : 0);
        return result;
    }
}
