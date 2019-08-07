/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.configuration;

/**
 * Service class with information about property in configuration state.
 */
public class FieldProcessingInfo {
    /** Property name. */
    private final String name;

    /** Count of occurrence in configuration class. */
    private int occurrence;

    /** Deprecated sign of property getter or setter method. */
    private boolean deprecated;

    /**
     * Constructor.
     *
     * @param name Property name.
     * @param occurrence Count of occurrence in configuration class.
     * @param deprecated Deprecated sign of property getter or setter method.
     */
    public FieldProcessingInfo(String name, int occurrence, boolean deprecated) {
        this.name = name;
        this.occurrence = occurrence;
        this.deprecated = deprecated;
    }

    /**
     * @return Property name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Count of occurrence in configuration class.
     */
    public int getOccurrence() {
        return occurrence;
    }

    /**
     * @return Deprecated sign of property getter or setter method.
     */
    public boolean isDeprecated() {
        return deprecated;
    }

    /**
     * Increase occurrence count.
     *
     * @return {@code this} for chaining.
     */
    public FieldProcessingInfo next() {
        occurrence += 1;

        return this;
    }

    /**
     * Set deprecated state.
     *
     * @param state Deprecated state of checked method.
     * @return {@code this} for chaining.
     */
    public FieldProcessingInfo deprecated(boolean state) {
        deprecated = deprecated || state;

        return this;
    }
}
