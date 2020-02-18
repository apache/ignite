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

package org.apache.ignite.internal.mxbean;

import java.lang.reflect.Method;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.NotCompliantMBeanException;
import org.apache.ignite.mxbean.MXBeanParameter;
import org.apache.ignite.mxbean.MXBeanParametersDescriptions;
import org.apache.ignite.mxbean.MXBeanParametersNames;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Contains tests for {@link IgniteStandardMXBean} methods.
 */
public class IgniteStandardMXBeanTest {
    /** */
    private static final String NAME = "Name";

    /** */
    private static final String TYPE = "Type";

    /** */
    private static final String OPERATION_INFO_DESCRIPTION = "Operation info description";

    /** */
    private static final String PARAMETER_INFO_DESCRIPTION = "Parameter info description";

    /** */
    private static final String EMPTY_STRING = "";

    /** */
    private static final String TEST_METHOD_1 = "testMethod1";

    /** */
    private static final String TEST_METHOD_2 = "testMethod2";

    /** */
    private static final String TEST_METHOD_3 = "testMethod3";

    /** */
    private static final String TEST_METHOD_4 = "testMethod4";

    /** */
    private static final String TEST_METHOD_5 = "testMethod5";

    /** */
    private static final String TEST_METHOD_6 = "testMethod6";

    /** */
    private static final String TEST_METHOD_7 = "testMethod7";

    /** */
    private static final String TEST_METHOD_8 = "testMethod8";

    /** */
    private static final String TEST_METHOD_9 = "testMethod9";

    /** */
    private static final String TEST_METHOD_10 = "testMethod10";

    /** */
    private static final String FIRST_DESCRIPTION_PARAM_ANNOTATION = "First description parameter annotation.";

    /** */
    private static final String FIRST_DESCRIPTION_METHOD_ANNOTATION = "First description method annotation.";

    /** */
    private static final String SECOND_DESCRIPTION_PARAM_ANNOTATION = "Second description parameter annotation.";

    /** */
    private static final String SECOND_DESCRIPTION_METHOD_ANNOTATION = "Second description method annotation.";

    /** */
    private static final String FIRST_NAME_PARAM_ANNOTATION = "First name parameter annotation";

    /** */
    private static final String FIRST_NAME_METHOD_ANNOTATION = "First name method annotation";

    /** */
    private static final String SECOND_NAME_PARAM_ANNOTATION = "Second name parameter annotation";

    /** */
    private static final String SECOND_NAME_METHOD_ANNOTATION = "Second name method annotation";

    /** */
    private static final String FIRST_LOWERCASE_LETTER = "first lowercase letter.";

    /** */
    private static final String NO_DOT_AT_THE_END_OF_THE_STRING = "No dot at the end of the string";

    /** */
    private static final int ZERO_INDEX = 0;

    /** */
    private static final int FIRST_INDEX = 1;

    /**
     * Instance of {@link IgniteStandardMXBean}.
     */
    private final IgniteStandardMXBean igniteStandardMXBean;

    /**
     * Instance of {@link MBeanParameterInfo}.
     */
    private final MBeanParameterInfo paramInfo;

    /**
     * Public constructor that initializes instances of IgniteStandardMXBean and MBeanParameterInfo classes.
     */
    public IgniteStandardMXBeanTest() throws NotCompliantMBeanException {
        TestInterfaceImpl testItfImpl = new TestInterfaceImpl();

        igniteStandardMXBean = new IgniteStandardMXBean(testItfImpl, TestInterface.class);

        paramInfo = new MBeanParameterInfo(NAME, TYPE, PARAMETER_INFO_DESCRIPTION);
    }

    /**
     * A test method that represents a situation in which the method has only old annotation.
     * All annotation parameters are valid.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test
    public void getDescription_OldAnnotation() throws NoSuchMethodException {
        String actualRes = getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_1, FIRST_INDEX);

        assertEquals(SECOND_DESCRIPTION_METHOD_ANNOTATION, actualRes);
    }

    /**
     * A test method that represents a situation in which the method has only old annotation.
     * An empty array is used as parameters in the annotation resulting in an AssertionError.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test(expected = AssertionError.class)
    public void getDescription_OldAnnotationEmptyValueArray() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_2, FIRST_INDEX);
    }

    /**
     * A test method that represents a situation in which the method has only old annotation.
     * An array whose length is less than transmitted parameter index is used as a parameter
     * resulting in an AssertionError.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test(expected = AssertionError.class)
    public void getDescription_OldAnnotationValueLengthLessThenParamIndex() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_3, FIRST_INDEX);
    }

    /**
     * A test method that represents a situation in which the method has only old annotation.
     * Empty description parameter value resulting in an AssertionError.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test(expected = AssertionError.class)
    public void getDescription_OldAnnotationEmptyParamValue() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_4, ZERO_INDEX);
    }

    /**
     * A test method that represents a situation in which the method has only old annotation.
     * Description parameter without a dot at the end of the sentence resulting in an AssertionError.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test(expected = AssertionError.class)
    public void getDescription_OldAnnotationNoDotAtTheEndOfTheString() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_5, ZERO_INDEX);
    }

    /**
     * A test method that represents a situation in which the method has only old annotation.
     * Description parameter starts with a lowercase letter resulting in an AssertionError.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test(expected = AssertionError.class)
    public void getDescription_OldAnnotationFirstLowercaseLetter() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_5, FIRST_INDEX);
    }

    /**
     * A test method that represents a situation in which the method has only new annotations.
     * All annotation parameters are valid.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test
    public void getDescription_NewAnnotation() throws NoSuchMethodException {
        String actualRes = getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_6, FIRST_INDEX);

        assertEquals(SECOND_DESCRIPTION_PARAM_ANNOTATION, actualRes);
    }

    /**
     * A test method that represents a situation in which the method has only new annotations.
     * Empty description parameter value resulting in an AssertionError.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test(expected = AssertionError.class)
    public void getDescription_NewAnnotationEmptyParamValue() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_7, ZERO_INDEX);
    }

    /**
     * A test method that represents a situation in which the method has only new annotation.
     * Description parameter without a dot at the end of the sentence resulting in an AssertionError.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test(expected = AssertionError.class)
    public void getDescription_NewAnnotationNoDotAtTheEndOfTheString() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_8, ZERO_INDEX);
    }

    /**
     * A test method that represents a situation in which the method has only new annotations.
     * Description parameter starts with a lowercase letter resulting in an AssertionError.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test(expected = AssertionError.class)
    public void getDescription_NewAnnotationFirstLowercaseLetter() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_8, FIRST_INDEX);
    }

    /**
     * A test method that represents a situation in which the method has both old and new annotations.
     * All annotations parameters are valid.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test
    public void getDescription_BothOldAndNewAnnotations() throws NoSuchMethodException {
        String actualRes = getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_9, FIRST_INDEX);

        assertEquals(SECOND_DESCRIPTION_METHOD_ANNOTATION, actualRes);
    }

    /**
     * A test method that represents a situation in which the method has no annotations at all.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test
    public void getDescription_NoAnnotations() throws NoSuchMethodException {
        String actualRes = getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_10, FIRST_INDEX);

        assertEquals(PARAMETER_INFO_DESCRIPTION, actualRes);
    }

    /**
     * A test method that represents a situation in which the method has only the old annotation.
     * All annotation parameters are valid.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test
    public void getParameterName_OldAnnotation() throws NoSuchMethodException {
        String actualRes = getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_1, FIRST_INDEX);

        assertEquals(SECOND_NAME_METHOD_ANNOTATION, actualRes);
    }

    /**
     * A test method that represents a situation in which the method has only old annotation.
     * An empty array is used as parameters in the annotation resulting in an AssertionError.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test(expected = AssertionError.class)
    public void getParameterName_OldAnnotationEmptyValueArray() throws NoSuchMethodException {
        getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_2, FIRST_INDEX);
    }

    /**
     * A test method that represents a situation in which the method has only old annotation.
     * An array whose length is less than transmitted parameter index is used as a parameter
     * resulting in an AssertionError.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test(expected = AssertionError.class)
    public void getParameterName_OldAnnotationValueLengthLessThenParamIndex() throws NoSuchMethodException {
        getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_3, FIRST_INDEX);
    }

    /**
     * A test method that represents a situation in which the method has only old annotation.
     * Empty parameter name value resulting in an AssertionError.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test(expected = AssertionError.class)
    public void getParameterName_OldAnnotationEmptyParamValue() throws NoSuchMethodException {
        getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_4, ZERO_INDEX);
    }

    /**
     * A test method that represents a situation in which the method has only new annotations.
     * All annotation parameters are valid.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test
    public void getParameterName_NewAnnotation() throws NoSuchMethodException {
        String actualRes = getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_6, FIRST_INDEX);

        assertEquals(SECOND_NAME_PARAM_ANNOTATION, actualRes);
    }

    /**
     * A test method that represents a situation in which the method has only new annotations.
     * Empty parameter name value resulting in an AssertionError.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test(expected = AssertionError.class)
    public void getParameterName_NewAnnotationEmptyParamValue() throws NoSuchMethodException {
        getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_7, ZERO_INDEX);
    }

    /**
     * A test method that represents a situation in which the method has both old and new annotations.
     * All annotations parameters are valid.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test
    public void getParameterName_BothOldAndNewAnnotations() throws NoSuchMethodException {
        String actualRes = getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_9, FIRST_INDEX);

        assertEquals(SECOND_NAME_METHOD_ANNOTATION, actualRes);
    }

    /**
     * A test method that represents a situation in which the method has no annotations at all.
     *
     * @throws NoSuchMethodException if method is not found.
     */
    @Test
    public void getParameterName_NoAnnotations() throws NoSuchMethodException {
        String actualRes = getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_10, FIRST_INDEX);

        assertEquals(NAME, actualRes);
    }

    /**
     * Utility method that returns the description for method argument by method name and parameter index.
     *
     * @param mName method name from interface TestInterface.
     * @param paramIdx the sequence number of the argument considered.
     * @return the description for method argument.
     * @throws NoSuchMethodException if a matching method is not found.
     */
    private String getDescriptionWithMethodNameAndParamIndex(String mName, int paramIdx)
        throws NoSuchMethodException {
        MBeanOperationInfo operationInfo = getMBeanOperationInfoWithMehtodName(mName);
        return igniteStandardMXBean.getDescription(operationInfo, paramInfo, paramIdx);
    }

    /**
     * Utility method that returns the name for method argument by method name and parameter index.
     *
     * @param mName method name from interface TestInterface.
     * @param paramIdx the sequence number of the argument considered.
     * @return the name for method argument.
     * @throws NoSuchMethodException if a matching method is not found.
     */
    private String getParameterNameWithMethodNameAndParamIndex(String mName, int paramIdx)
        throws NoSuchMethodException {
        MBeanOperationInfo operationInfo = getMBeanOperationInfoWithMehtodName(mName);
        return igniteStandardMXBean.getParameterName(operationInfo, paramInfo, paramIdx);
    }

    /**
     * Utility method for getting instance of MBeanOperationInfo constructed with default description from TestInterface
     * by its method name.
     *
     * @param mName method name from interface TestInterface.
     * @return MBeanOperationInfo.
     * @throws NoSuchMethodException if a matching method is not found.
     */
    private MBeanOperationInfo getMBeanOperationInfoWithMehtodName(String mName) throws NoSuchMethodException {
        Method m = TestInterface.class.getDeclaredMethod(mName, String.class, String.class);
        return new MBeanOperationInfo(OPERATION_INFO_DESCRIPTION, m);
    }

    /**
     * Interface used for testing.
     */
    public static interface TestInterface {
        /**
         * Method used for testing.
         *
         * @param firstParam first string method parameter.
         * @param secondParam second string method parameter.
         */
        @MXBeanParametersNames({FIRST_NAME_METHOD_ANNOTATION, SECOND_NAME_METHOD_ANNOTATION})
        @MXBeanParametersDescriptions({FIRST_DESCRIPTION_METHOD_ANNOTATION, SECOND_DESCRIPTION_METHOD_ANNOTATION})
        void testMethod1(String firstParam, String secondParam);

        /**
         * Method used for testing.
         *
         * @param firstParam first string method parameter.
         * @param secondParam second string method parameter.
         */
        @MXBeanParametersNames({})
        @MXBeanParametersDescriptions({})
        void testMethod2(String firstParam, String secondParam);

        /**
         * Method used for testing.
         *
         * @param firstParam first string method parameter.
         * @param secondParam second string method parameter.
         */
        @MXBeanParametersNames({FIRST_NAME_METHOD_ANNOTATION})
        @MXBeanParametersDescriptions({FIRST_DESCRIPTION_METHOD_ANNOTATION})
        void testMethod3(String firstParam, String secondParam);

        /**
         * Method used for testing.
         *
         * @param firstParam first string method parameter.
         * @param secondParam second string method parameter.
         */
        @MXBeanParametersNames({EMPTY_STRING})
        @MXBeanParametersDescriptions({EMPTY_STRING})
        void testMethod4(String firstParam, String secondParam);

        /**
         * Method used for testing.
         *
         * @param firstParam first string method parameter.
         * @param secondParam second string method parameter.
         */
        @MXBeanParametersNames({FIRST_NAME_METHOD_ANNOTATION, SECOND_NAME_METHOD_ANNOTATION})
        @MXBeanParametersDescriptions({NO_DOT_AT_THE_END_OF_THE_STRING, FIRST_LOWERCASE_LETTER})
        void testMethod5(String firstParam, String secondParam);

        /**
         * Method used for testing.
         *
         * @param firstParam first string method parameter.
         * @param secondParam second string method parameter.
         */
        void testMethod6(
            @MXBeanParameter(name = FIRST_NAME_PARAM_ANNOTATION, description = FIRST_DESCRIPTION_PARAM_ANNOTATION)
                String firstParam,
            @MXBeanParameter(name = SECOND_NAME_PARAM_ANNOTATION, description = SECOND_DESCRIPTION_PARAM_ANNOTATION)
                String secondParam
        );

        /**
         * Method used for testing.
         *
         * @param firstParam first string method parameter.
         * @param secondParam second string method parameter.
         */
        void testMethod7(
            @MXBeanParameter(name = EMPTY_STRING, description = EMPTY_STRING)
                String firstParam,
            @MXBeanParameter(name = SECOND_NAME_PARAM_ANNOTATION, description = SECOND_DESCRIPTION_PARAM_ANNOTATION)
                String secondParam
        );

        /**
         * Method used for testing.
         *
         * @param firstParam first string method parameter.
         * @param secondParam second string method parameter.
         */
        void testMethod8(
            @MXBeanParameter(name = FIRST_NAME_PARAM_ANNOTATION, description = NO_DOT_AT_THE_END_OF_THE_STRING)
                String firstParam,
            @MXBeanParameter(name = SECOND_NAME_PARAM_ANNOTATION, description = FIRST_LOWERCASE_LETTER)
                String secondParam
        );

        /**
         * Method used for testing.
         *
         * @param firstParam first string method parameter.
         * @param secondParam second string method parameter.
         */
        @MXBeanParametersNames({FIRST_NAME_METHOD_ANNOTATION, SECOND_NAME_METHOD_ANNOTATION})
        @MXBeanParametersDescriptions({FIRST_DESCRIPTION_METHOD_ANNOTATION, SECOND_DESCRIPTION_METHOD_ANNOTATION})
        void testMethod9(
            @MXBeanParameter(name = FIRST_NAME_PARAM_ANNOTATION, description = NO_DOT_AT_THE_END_OF_THE_STRING)
                String firstParam,
            @MXBeanParameter(name = SECOND_NAME_PARAM_ANNOTATION, description = FIRST_LOWERCASE_LETTER)
                String secondParam
        );

        /**
         * Method used for testing.
         *
         * @param firstParam first string method parameter.
         * @param secondParam second string method parameter.
         */
        void testMethod10(String firstParam, String secondParam);
    }

    /**
     * Test interface implementation.
     */
    private static class TestInterfaceImpl implements TestInterface {
        /** {@inheritDoc} */
        @Override public void testMethod1(String firstParam, String secondParam) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void testMethod2(String firstParam, String secondParam) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void testMethod3(String firstParam, String secondParam) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void testMethod4(String firstParam, String secondParam) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void testMethod5(String firstParam, String secondParam) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void testMethod6(String firstParam, String secondParam) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void testMethod7(String firstParam, String secondParam) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void testMethod8(String firstParam, String secondParam) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void testMethod9(String firstParam, String secondParam) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void testMethod10(String firstParam, String secondParam) {
            // No-op.
        }
    }
}
