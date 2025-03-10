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

package org.apache.ignite.compatibility;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.NotNull;

/** Released Ignite versions. */
@SuppressWarnings("unused")
public enum IgniteReleasedVersion {
    /** */
    VER_2_1_0("2.1.0"),

    /** */
    VER_2_2_0("2.2.0"),

    /** */
    VER_2_3_0("2.3.0"),

    /** */
    VER_2_4_0("2.4.0"),

    /** */
    VER_2_5_0("2.5.0"),

    /** */
    VER_2_6_0("2.6.0"),

    /** */
    VER_2_7_0("2.7.0"),

    /** */
    VER_2_7_6("2.7.6"),

    /** */
    VER_2_8_0("2.8.0"),

    /** */
    VER_2_8_1("2.8.1"),

    /** */
    VER_2_9_0("2.9.0"),

    /** */
    VER_2_9_1("2.9.1"),

    /** */
    VER_2_10_0("2.10.0"),

    /** */
    VER_2_11_0("2.11.0"),

    /** */
    VER_2_12_0("2.12.0"),

    /** */
    VER_2_13_0("2.13.0"),

    /** */
    VER_2_14_0("2.14.0"),

    /** */
    VER_2_15_0("2.15.0"),

    /** */
    VER_2_16_0("2.16.0"),
    
    /** */
    VER_2_17_0("2.17.0");

    /** Ignite version. */
    private final IgniteProductVersion ver;

    /** @param ver Ignite version. */
    IgniteReleasedVersion(String ver) {
        this.ver = IgniteProductVersion.fromString(ver);
    }

    /** @return Ignite version. */
    public IgniteProductVersion version() {
        return ver;
    }

    /**
     * @return Ignite versions since provided version.
     * @param ver Version.
     */
    public static Collection<String> since(IgniteReleasedVersion ver) {
        return F.transform(F.view(F.asList(values()), v -> v.version().compareTo(ver.version()) >= 0),
            IgniteReleasedVersion::toString);
    }

    /**
     *
     */
    public static @NotNull IgniteReleasedVersion fromString(String ver) {
        IgniteProductVersion productVer = IgniteProductVersion.fromString(ver);

        Optional<IgniteReleasedVersion> res = Arrays.stream(values()).filter(el -> el.ver.equals(productVer)).findFirst();

        if (!res.isPresent())
            throw new IgniteException("Provided version has been never released [version=" + ver);

        return res.get();
    }

    /** @return String representation of three-part version number. */
    @Override public String toString() {
        return ver.major() + "." + ver.minor() + "." + ver.maintenance();
    }
}
