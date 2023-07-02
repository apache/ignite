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

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Message source which support wildcards in basenames.
 */
public class WildcardReloadableResourceBundleMessageSource extends ReloadableResourceBundleMessageSource {
    /**
     * Constant to define the properties file extension.
     */
    private static final String PROPERTIES_SUFFIX = ".properties";

    /**
     * Constant to define the classes string in the URI.
     */
    private static final String CLASSES = "classes/";

    /**
     * Constant to define the class path string.
     */
    private static final String CLASS_PATH = "classpath:";

    /**
     * Variable reference to PathMatching resource.
     */
    private ResourcePatternResolver ptrnRslvr = new PathMatchingResourcePatternResolver();

    /** {@inheritDoc} */
    @Override public void setBasenames(String... passedInBaseNames) {
        if (passedInBaseNames != null) {
            List<String> baseNames = new ArrayList<>();

            Stream.of(passedInBaseNames)
                    .filter(StringUtils::isNotBlank)
                    .map(this::getResources)
                    .flatMap(this::getBaseNames)
                    .forEach(baseNames::add);

            super.setBasenames(baseNames.toArray(new String[0]));
        }
    }

    /**
     * @param name Name.
     *
     * @return Resources.
     */
    private Resource[] getResources(String name) {
        try {
            return ptrnRslvr.getResources(name);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * @param resources Resource.
     *
     * @return Basename.
     */
    private Stream<String> getBaseNames(Resource[] resources) {
        return Stream.of(resources).map(this::getBaseName);
    }

    /**
     * @param rsrc Resource.
     *
     * @return Basename.
     */
    private String getBaseName(Resource rsrc) {
        try {
            String baseName = null;
            String uri = rsrc.getURI().toString();

            if (!uri.endsWith(PROPERTIES_SUFFIX))
                return baseName;

            if (rsrc instanceof ClassPathResource)
                baseName = StringUtils.substringBefore(uri, PROPERTIES_SUFFIX);
            else if (rsrc instanceof UrlResource)
                baseName = CLASS_PATH + StringUtils.substringBetween(uri, ".jar!/", PROPERTIES_SUFFIX);
            else
                baseName = CLASS_PATH + StringUtils.substringBetween(uri, CLASSES, PROPERTIES_SUFFIX);

            return baseName;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
