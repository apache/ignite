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

/**
 * {@link IndexException} to be thrown if <a href="https://projects.eclipse.org/projects/locationtech.jts">Java Topology Suite (JTS)</a>
 * library is not found in classpath
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class JTSNotFoundException extends IndexException {

    private static final String MESSAGE = "JTS JAR is not provided due to license compatibility issues, please " +
                                          "include jts-core-1.15.0.jar in project lib directory in order to use " +
                                          "GeoShapeMapper or GeoShapeCondition";

    /**
     * Default constructor.
     */
    public JTSNotFoundException() {
        super(MESSAGE);
    }
}
