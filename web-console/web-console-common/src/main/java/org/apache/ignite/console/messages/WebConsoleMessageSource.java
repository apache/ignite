/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.messages;

import java.util.Locale;

/**
 * Web console message source.
 */
public class WebConsoleMessageSource extends WildcardReloadableResourceBundleMessageSource {
    /** Message source. */
    private static final WebConsoleMessageSource msgSrc = new WebConsoleMessageSource("UTF-8", "classpath*:i18/*");

    /** Accessor. */
    private static final WebConsoleMessageSourceAccessor accessor = new WebConsoleMessageSourceAccessor(msgSrc, Locale.US);

    /**
     * @param baseNames Base names.
     */
    public WebConsoleMessageSource(String encoding, String... baseNames) {
        setDefaultEncoding(encoding);
        setBasenames(baseNames);
    }

    /**
     * Return message source accessor for web console messages.
     *
     * @return Message Source Accessor.
     */
    public static WebConsoleMessageSourceAccessor getAccessor() {
        return accessor;
    }
}
