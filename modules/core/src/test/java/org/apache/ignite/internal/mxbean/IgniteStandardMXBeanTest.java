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
import org.apache.ignite.mxbean.MXBeanParameterInfo;
import org.apache.ignite.mxbean.MXBeanParametersDescriptions;
import org.apache.ignite.mxbean.MXBeanParametersNames;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IgniteStandardMXBeanTest {

    private static final String NAME = "Name";
    private static final String TYPE = "Type";
    private static final String OPERATION_INFO_DESCRIPTION = "Operation info description";
    private static final String PARAMETER_INFO_DESCRIPTION = "Parameter info description";

    private static final String EMPTY_STRING = "";

    private static final String TEST_METHOD_1 = "testMethod1";
    private static final String TEST_METHOD_2 = "testMethod2";
    private static final String TEST_METHOD_3 = "testMethod3";
    private static final String TEST_METHOD_4 = "testMethod4";
    private static final String TEST_METHOD_5 = "testMethod5";
    private static final String TEST_METHOD_6 = "testMethod6";
    private static final String TEST_METHOD_7 = "testMethod7";
    private static final String TEST_METHOD_8 = "testMethod8";
    private static final String TEST_METHOD_9 = "testMethod9";
    private static final String TEST_METHOD_10 = "testMethod10";

    private static final String FIRST_DESCRIPTION_PARAM_ANNOTATION = "First description parameter annotation.";
    private static final String FIRST_DESCRIPTION_METHOD_ANNOTATION = "First description method annotation.";
    private static final String SECOND_DESCRIPTION_PARAM_ANNOTATION = "Second description parameter annotation.";
    private static final String SECOND_DESCRIPTION_METHOD_ANNOTATION = "Second description method annotation.";

    private static final String FIRST_NAME_PARAM_ANNOTATION = "First name parameter annotation";
    private static final String FIRST_NAME_METHOD_ANNOTATION = "First name method annotation";
    private static final String SECOND_NAME_PARAM_ANNOTATION = "Second name parameter annotation";
    private static final String SECOND_NAME_METHOD_ANNOTATION = "Second name method annotation";

    private static final String FIRST_LOWERCASE_LETTER = "first lowercase letter.";
    private static final String NO_DOT_AT_THE_END_OF_THE_STRING = "No dot at the end of the string";

    private static final int ZERO_INDEX = 0;
    private static final int FIRST_INDEX = 1;

    private final IgniteStandardMXBean igniteStandardMXBean;
    private final MBeanParameterInfo parameterInfo;

    public IgniteStandardMXBeanTest() throws NotCompliantMBeanException {
        TestInterfaceImpl testInterfaceImpl = new TestInterfaceImpl();
        igniteStandardMXBean = new IgniteStandardMXBean(testInterfaceImpl, TestInterface.class);
        parameterInfo = new MBeanParameterInfo(NAME, TYPE, PARAMETER_INFO_DESCRIPTION);
    }

    @Test
    public void getDescription_OldAnnotation() throws NoSuchMethodException {
        String actualResult = getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_1, FIRST_INDEX);

        assertEquals(SECOND_DESCRIPTION_METHOD_ANNOTATION, actualResult);
    }

    @Test(expected = AssertionError.class)
    public void getDescription_OldAnnotationEmptyValueArray() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_2, FIRST_INDEX);
    }

    @Test(expected = AssertionError.class)
    public void getDescription_OldAnnotationValueLengthLessThenParamIndex() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_3, FIRST_INDEX);
    }

    @Test(expected = AssertionError.class)
    public void getDescription_OldAnnotationEmptyParamValue() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_4, ZERO_INDEX);
    }

    @Test(expected = AssertionError.class)
    public void getDescription_OldAnnotationNoDotAtTheEndOfTheString() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_5, ZERO_INDEX);
    }

    @Test(expected = AssertionError.class)
    public void getDescription_OldAnnotationFirstLowercaseLetter() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_5, FIRST_INDEX);
    }

    @Test
    public void getDescription_NewAnnotation() throws NoSuchMethodException {
        String actualResult = getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_6, FIRST_INDEX);

        assertEquals(SECOND_DESCRIPTION_PARAM_ANNOTATION, actualResult);
    }

    @Test(expected = AssertionError.class)
    public void getDescription_NewAnnotationEmptyParamValue() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_7, ZERO_INDEX);
    }

    @Test(expected = AssertionError.class)
    public void getDescription_NewAnnotationNoDotAtTheEndOfTheString() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_8, ZERO_INDEX);
    }

    @Test(expected = AssertionError.class)
    public void getDescription_NewAnnotationFirstLowercaseLetter() throws NoSuchMethodException {
        getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_8, FIRST_INDEX);
    }

    @Test
    public void getDescription_BothOldAndNewAnnotations() throws NoSuchMethodException {
        String actualResult = getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_9, FIRST_INDEX);

        assertEquals(SECOND_DESCRIPTION_METHOD_ANNOTATION, actualResult);
    }

    @Test
    public void getDescription_NoAnnotations() throws NoSuchMethodException {
        String actualResult = getDescriptionWithMethodNameAndParamIndex(TEST_METHOD_10, FIRST_INDEX);

        assertEquals(PARAMETER_INFO_DESCRIPTION, actualResult);
    }

    @Test
    public void getParameterName_OldAnnotation() throws NoSuchMethodException {
        String actualResult = getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_1, FIRST_INDEX);

        assertEquals(SECOND_NAME_METHOD_ANNOTATION, actualResult);
    }

    @Test(expected = AssertionError.class)
    public void getParameterName_OldAnnotationEmptyValueArray() throws NoSuchMethodException {
        getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_2, FIRST_INDEX);
    }

    @Test(expected = AssertionError.class)
    public void getParameterName_OldAnnotationValueLengthLessThenParamIndex() throws NoSuchMethodException {
        getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_3, FIRST_INDEX);
    }

    @Test(expected = AssertionError.class)
    public void getParameterName_OldAnnotationEmptyParamValue() throws NoSuchMethodException {
        getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_4, ZERO_INDEX);
    }

    @Test
    public void getParameterName_NewAnnotation() throws NoSuchMethodException {
        String actualResult = getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_6, FIRST_INDEX);

        assertEquals(SECOND_NAME_PARAM_ANNOTATION, actualResult);
    }

    @Test(expected = AssertionError.class)
    public void getParameterName_NewAnnotationEmptyParamValue() throws NoSuchMethodException {
        getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_7, ZERO_INDEX);
    }

    @Test
    public void getParameterName_BothOldAndNewAnnotations() throws NoSuchMethodException {
        String actualResult = getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_9, FIRST_INDEX);

        assertEquals(SECOND_NAME_METHOD_ANNOTATION, actualResult);
    }

    @Test
    public void getParameterName_NoAnnotations() throws NoSuchMethodException {
        String actualResult = getParameterNameWithMethodNameAndParamIndex(TEST_METHOD_10, FIRST_INDEX);

        assertEquals(NAME, actualResult);
    }

    private String getDescriptionWithMethodNameAndParamIndex(String methodName, int paramIndex)
        throws NoSuchMethodException {
        MBeanOperationInfo operationInfo = getMBeanOperationInfoWithMehtodName(methodName);
        return igniteStandardMXBean.getDescription(operationInfo, parameterInfo, paramIndex);
    }

    private String getParameterNameWithMethodNameAndParamIndex(String methodName, int paramIndex)
        throws NoSuchMethodException {
        MBeanOperationInfo operationInfo = getMBeanOperationInfoWithMehtodName(methodName);
        return igniteStandardMXBean.getParameterName(operationInfo, parameterInfo, paramIndex);
    }

    private MBeanOperationInfo getMBeanOperationInfoWithMehtodName(String methodName) throws NoSuchMethodException {
        Method method = TestInterface.class.getDeclaredMethod(methodName, String.class, String.class);
        return new MBeanOperationInfo(OPERATION_INFO_DESCRIPTION, method);
    }

    public static interface TestInterface {

        @MXBeanParametersNames({FIRST_NAME_METHOD_ANNOTATION, SECOND_NAME_METHOD_ANNOTATION})
        @MXBeanParametersDescriptions({FIRST_DESCRIPTION_METHOD_ANNOTATION, SECOND_DESCRIPTION_METHOD_ANNOTATION})
        void testMethod1(String firstParam, String secondParam);

        @MXBeanParametersNames({})
        @MXBeanParametersDescriptions({})
        void testMethod2(String firstParam, String secondParam);

        @MXBeanParametersNames({FIRST_NAME_METHOD_ANNOTATION})
        @MXBeanParametersDescriptions({FIRST_DESCRIPTION_METHOD_ANNOTATION})
        void testMethod3(String firstParam, String secondParam);

        @MXBeanParametersNames({EMPTY_STRING})
        @MXBeanParametersDescriptions({EMPTY_STRING})
        void testMethod4(String firstParam, String secondParam);

        @MXBeanParametersNames({FIRST_NAME_METHOD_ANNOTATION, SECOND_NAME_METHOD_ANNOTATION})
        @MXBeanParametersDescriptions({NO_DOT_AT_THE_END_OF_THE_STRING, FIRST_LOWERCASE_LETTER})
        void testMethod5(String firstParam, String secondParam);

        void testMethod6(
            @MXBeanParameterInfo(name = FIRST_NAME_PARAM_ANNOTATION, description = FIRST_DESCRIPTION_PARAM_ANNOTATION)
                String firstParam,
            @MXBeanParameterInfo(name = SECOND_NAME_PARAM_ANNOTATION, description = SECOND_DESCRIPTION_PARAM_ANNOTATION)
                String secondParam
        );

        void testMethod7(
            @MXBeanParameterInfo(name = EMPTY_STRING, description = EMPTY_STRING)
                String firstParam,
            @MXBeanParameterInfo(name = SECOND_NAME_PARAM_ANNOTATION, description = SECOND_DESCRIPTION_PARAM_ANNOTATION)
                String secondParam
        );

        void testMethod8(
            @MXBeanParameterInfo(name = FIRST_NAME_PARAM_ANNOTATION, description = NO_DOT_AT_THE_END_OF_THE_STRING)
                String firstParam,
            @MXBeanParameterInfo(name = SECOND_NAME_PARAM_ANNOTATION, description = FIRST_LOWERCASE_LETTER)
                String secondParam
        );

        @MXBeanParametersNames({FIRST_NAME_METHOD_ANNOTATION, SECOND_NAME_METHOD_ANNOTATION})
        @MXBeanParametersDescriptions({FIRST_DESCRIPTION_METHOD_ANNOTATION, SECOND_DESCRIPTION_METHOD_ANNOTATION})
        void testMethod9(
            @MXBeanParameterInfo(name = FIRST_NAME_PARAM_ANNOTATION, description = NO_DOT_AT_THE_END_OF_THE_STRING)
                String firstParam,
            @MXBeanParameterInfo(name = SECOND_NAME_PARAM_ANNOTATION, description = FIRST_LOWERCASE_LETTER)
                String secondParam
        );

        void testMethod10(String firstParam, String secondParam);
    }

    private static class TestInterfaceImpl implements TestInterface {

        @Override public void testMethod1(String firstParam, String secondParam) {
        }

        @Override public void testMethod2(String firstParam, String secondParam) {
        }

        @Override public void testMethod3(String firstParam, String secondParam) {
        }

        @Override public void testMethod4(String firstParam, String secondParam) {
        }

        @Override public void testMethod5(String firstParam, String secondParam) {
        }

        @Override public void testMethod6(String firstParam, String secondParam) {
        }

        @Override public void testMethod7(String firstParam, String secondParam) {
        }

        @Override public void testMethod8(String firstParam, String secondParam) {
        }

        @Override public void testMethod9(String firstParam, String secondParam) {
        }

        @Override public void testMethod10(String firstParam, String secondParam) {
        }
    }
}
