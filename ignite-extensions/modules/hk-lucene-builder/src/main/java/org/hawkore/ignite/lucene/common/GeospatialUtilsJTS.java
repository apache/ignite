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
 
import org.apache.commons.lang3.StringUtils;
import org.hawkore.ignite.lucene.IndexException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;



/**
 * Utilities for Java Topology Suite (JTS) related stuff.
 *
 * <p> This class depends on <a href="https://projects.eclipse.org/projects/locationtech.jts">Java Topology
 * Suite (JTS)</a>. This library can't be distributed together with this project due to license compatibility problems,
 * but you can add it by putting <a href="http://search.maven.org/remotecontent?filepath=org/locationtech/jts/jts-core/1.15.0/jts-core-1.15.0.jar">jts-core-1.15.0.jar</a>
 * into project lib directory. 
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeospatialUtilsJTS extends GeospatialUtils {

    /** The spatial context to be used. */
    public static final JtsSpatialContext CONTEXT = JtsSpatialContext.GEO;

    /**
     * Returns the {@link JtsGeometry} represented by the specified WKT text.
     *
     * @param string the WKT text
     * @return the parsed geometry
     */
    public static JtsGeometry geometry(String string) {
        if (StringUtils.isBlank(string)) {
            throw new IndexException("Shape shouldn't be blank");
        }
        try {
            GeometryFactory geometryFactory = CONTEXT.getGeometryFactory();
            WKTReader reader = new WKTReader(geometryFactory);
            Geometry geometry = reader.read(string);
            if (!geometry.isValid()) {
                geometry = geometry.buffer(0);
            }
            return CONTEXT.makeShape(geometry);
        } catch (ParseException | IllegalArgumentException e) {
            throw new IndexException(e, "Shape '{}' is not parseable", string);
        }
    }
}
