/*
 *  Copyright 2005 The Apache Software Foundation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ignite.console.agent.db;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author Jeff Butler
 */
public class FullyQualifiedJavaType implements Comparable<FullyQualifiedJavaType> {
    private static final String JAVA_LANG = "java.lang";
    private static FullyQualifiedJavaType intInstance = null;
    private static FullyQualifiedJavaType stringInstance = null;
    private static FullyQualifiedJavaType booleanPrimitiveInstance = null;
    private static FullyQualifiedJavaType objectInstance = null;
    private static FullyQualifiedJavaType dateInstance = null;
    private static FullyQualifiedJavaType criteriaInstance = null;
    private static FullyQualifiedJavaType generatedCriteriaInstance = null;

    private String baseShortName;

    private String baseQualifiedName;

    private boolean explicitlyImported;
    private String packageName;
    private boolean primitive;
    private boolean isArray;
    private PrimitiveTypeWrapper primitiveTypeWrapper;
    private List<FullyQualifiedJavaType> typeArguments;

    private boolean wildcardType;
    private boolean boundedWildcard;
    private boolean extendsBoundedWildcard;

    public FullyQualifiedJavaType(String fullTypeSpecification) {
        super();
        typeArguments = new ArrayList<FullyQualifiedJavaType>();
        parse(fullTypeSpecification);
    }

    public static final FullyQualifiedJavaType getIntInstance() {
        if (intInstance == null) {
            intInstance = new FullyQualifiedJavaType("int");
        }

        return intInstance;
    }

    public static final FullyQualifiedJavaType getNewMapInstance() {
        return new FullyQualifiedJavaType("java.util.Map");
    }

    public static final FullyQualifiedJavaType getNewListInstance() {
        return new FullyQualifiedJavaType("java.util.List");
    }

    public static final FullyQualifiedJavaType getNewHashMapInstance() {
        return new FullyQualifiedJavaType("java.util.HashMap");
    }

    public static final FullyQualifiedJavaType getNewArrayListInstance() {
        return new FullyQualifiedJavaType("java.util.ArrayList");
    }

    public static final FullyQualifiedJavaType getNewIteratorInstance() {
        return new FullyQualifiedJavaType("java.util.Iterator");
    }

    public static final FullyQualifiedJavaType getStringInstance() {
        if (stringInstance == null) {
            stringInstance = new FullyQualifiedJavaType("java.lang.String");
        }

        return stringInstance;
    }

    public static final FullyQualifiedJavaType getBooleanPrimitiveInstance() {
        if (booleanPrimitiveInstance == null) {
            booleanPrimitiveInstance = new FullyQualifiedJavaType("boolean");
        }

        return booleanPrimitiveInstance;
    }

    public static final FullyQualifiedJavaType getObjectInstance() {
        if (objectInstance == null) {
            objectInstance = new FullyQualifiedJavaType("java.lang.Object");
        }

        return objectInstance;
    }

    public static final FullyQualifiedJavaType getDateInstance() {
        if (dateInstance == null) {
            dateInstance = new FullyQualifiedJavaType("java.util.Date");
        }

        return dateInstance;
    }

    public static final FullyQualifiedJavaType getCriteriaInstance() {
        if (criteriaInstance == null) {
            criteriaInstance = new FullyQualifiedJavaType("Criteria");
        }

        return criteriaInstance;
    }

    public static final FullyQualifiedJavaType getGeneratedCriteriaInstance() {
        if (generatedCriteriaInstance == null) {
            generatedCriteriaInstance = new FullyQualifiedJavaType(
                    "GeneratedCriteria");
        }

        return generatedCriteriaInstance;
    }

    public static boolean stringHasValue(String s) {
        return s != null && s.length() > 0;
    }

    private static String getPackage(String baseQualifiedName) {
        int index = baseQualifiedName.lastIndexOf('.');
        return baseQualifiedName.substring(0, index);
    }

    public boolean isExplicitlyImported() {
        return explicitlyImported;
    }

    public String getFullyQualifiedName() {
        StringBuilder sb = new StringBuilder();
        if (wildcardType) {
            sb.append('?');
            if (boundedWildcard) {
                if (extendsBoundedWildcard) {
                    sb.append(" extends ");
                } else {
                    sb.append(" super ");
                }

                sb.append(baseQualifiedName);
            }
        } else {
            sb.append(baseQualifiedName);
        }

        if (typeArguments.size() > 0) {
            boolean first = true;
            sb.append('<');
            for (FullyQualifiedJavaType fqjt : typeArguments) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append(fqjt.getFullyQualifiedName());

            }
            sb.append('>');
        }

        return sb.toString();
    }

    public List<String> getImportList() {
        List<String> answer = new ArrayList<String>();
        if (isExplicitlyImported()) {
            int index = baseShortName.indexOf('.');
            if (index == -1) {
                answer.add(baseQualifiedName);
            } else {
                // an inner class is specified, only import the top
                // level class
                StringBuilder sb = new StringBuilder();
                sb.append(packageName);
                sb.append('.');
                sb.append(baseShortName.substring(0, index));
                answer.add(sb.toString());
            }
        }

        for (FullyQualifiedJavaType fqjt : typeArguments) {
            answer.addAll(fqjt.getImportList());
        }

        return answer;
    }

    public String getPackageName() {
        return packageName;
    }

    public String getShortName() {
        StringBuilder sb = new StringBuilder();
        if (wildcardType) {
            sb.append('?');
            if (boundedWildcard) {
                if (extendsBoundedWildcard) {
                    sb.append(" extends ");
                } else {
                    sb.append(" super ");
                }

                sb.append(baseShortName);
            }
        } else {
            sb.append(baseShortName);
        }

        if (typeArguments.size() > 0) {
            boolean first = true;
            sb.append('<');
            for (FullyQualifiedJavaType fqjt : typeArguments) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append(fqjt.getShortName());

            }
            sb.append('>');
        }

        return sb.toString();
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof FullyQualifiedJavaType)) {
            return false;
        }

        FullyQualifiedJavaType other = (FullyQualifiedJavaType) obj;

        return getFullyQualifiedName().equals(other.getFullyQualifiedName());
    }

    public int hashCode() {
        return getFullyQualifiedName().hashCode();
    }

    public String toString() {
        return getFullyQualifiedName();
    }

    public boolean isPrimitive() {
        return primitive;
    }

    public PrimitiveTypeWrapper getPrimitiveTypeWrapper() {
        return primitiveTypeWrapper;
    }

    public int compareTo(FullyQualifiedJavaType other) {
        return getFullyQualifiedName().compareTo(other.getFullyQualifiedName());
    }

    public void addTypeArgument(FullyQualifiedJavaType type) {
        typeArguments.add(type);
    }

    private void parse(String fullTypeSpecification) {
        String spec = fullTypeSpecification.trim();

        if (spec.startsWith("?")) {
            wildcardType = true;
            spec = spec.substring(1).trim();
            if (spec.startsWith("extends ")) {
                boundedWildcard = true;
                extendsBoundedWildcard = true;
                spec = spec.substring(8);  // "extends ".length()
            } else if (spec.startsWith("super ")) {
                boundedWildcard = true;
                extendsBoundedWildcard = false;
                spec = spec.substring(6);  // "super ".length()
            } else {
                boundedWildcard = false;
            }
            parse(spec);
        } else {
            int index = fullTypeSpecification.indexOf('<');
            if (index == -1) {
                simpleParse(fullTypeSpecification);
            } else {
                simpleParse(fullTypeSpecification.substring(0, index));
                int endIndex = fullTypeSpecification.lastIndexOf('>');
                if (endIndex == -1) {
                    throw new RuntimeException("Invalid Type Specification: " + fullTypeSpecification);
                }
                genericParse(fullTypeSpecification.substring(index, endIndex + 1));
            }
            isArray = fullTypeSpecification.endsWith("]");
        }
    }

    private void simpleParse(String typeSpecification) {
        baseQualifiedName = typeSpecification.trim();
        if (baseQualifiedName.contains(".")) {
            packageName = getPackage(baseQualifiedName);
            baseShortName = baseQualifiedName
                    .substring(packageName.length() + 1);
            int index = baseShortName.lastIndexOf('.');
            if (index != -1) {
                baseShortName = baseShortName.substring(index + 1);
            }

            if (JAVA_LANG.equals(packageName)) {
                explicitlyImported = false;
            } else {
                explicitlyImported = true;
            }
        } else {
            baseShortName = baseQualifiedName;
            explicitlyImported = false;
            packageName = "";

            if ("byte".equals(baseQualifiedName)) {
                primitive = true;
                primitiveTypeWrapper = PrimitiveTypeWrapper.getByteInstance();
            } else if ("short".equals(baseQualifiedName)) {
                primitive = true;
                primitiveTypeWrapper = PrimitiveTypeWrapper.getShortInstance();
            } else if ("int".equals(baseQualifiedName)) {
                primitive = true;
                primitiveTypeWrapper = PrimitiveTypeWrapper
                        .getIntegerInstance();
            } else if ("long".equals(baseQualifiedName)) {
                primitive = true;
                primitiveTypeWrapper = PrimitiveTypeWrapper.getLongInstance();
            } else if ("char".equals(baseQualifiedName)) {
                primitive = true;
                primitiveTypeWrapper = PrimitiveTypeWrapper
                        .getCharacterInstance();
            } else if ("float".equals(baseQualifiedName)) {
                primitive = true;
                primitiveTypeWrapper = PrimitiveTypeWrapper.getFloatInstance();
            } else if ("double".equals(baseQualifiedName)) {
                primitive = true;
                primitiveTypeWrapper = PrimitiveTypeWrapper.getDoubleInstance();
            } else if ("boolean".equals(baseQualifiedName)) {
                primitive = true;
                primitiveTypeWrapper = PrimitiveTypeWrapper
                        .getBooleanInstance();
            } else {
                primitive = false;
                primitiveTypeWrapper = null;
            }
        }
    }

    private void genericParse(String genericSpecification) {
        int lastIndex = genericSpecification.lastIndexOf('>');
        if (lastIndex == -1) {
            throw new RuntimeException("Invalid Type Specification: " + genericSpecification);
        }
        String argumentString = genericSpecification.substring(1, lastIndex);
        // need to find "," outside of a <> bounds
        StringTokenizer st = new StringTokenizer(argumentString, ",<>", true);
        int openCount = 0;
        StringBuilder sb = new StringBuilder();
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if ("<".equals(token)) {
                sb.append(token);
                openCount++;
            } else if (">".equals(token)) {
                sb.append(token);
                openCount--;
            } else if (",".equals(token)) {
                if (openCount == 0) {
                    typeArguments
                            .add(new FullyQualifiedJavaType(sb.toString()));
                    sb.setLength(0);
                } else {
                    sb.append(token);
                }
            } else {
                sb.append(token);
            }
        }

        if (openCount != 0) {
            throw new RuntimeException("Invalid Type Specification: " + genericSpecification);
        }

        String finalType = sb.toString();
        if (stringHasValue(finalType)) {
            typeArguments.add(new FullyQualifiedJavaType(finalType));
        }
    }

    public boolean isArray() {
        return isArray;
    }
}
