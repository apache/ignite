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

package org.apache.ignite.testframework.junits;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import sun.jvmstat.monitor.HostIdentifier;
import sun.jvmstat.monitor.Monitor;
import sun.jvmstat.monitor.MonitoredHost;
import sun.jvmstat.monitor.MonitoredVm;
import sun.jvmstat.monitor.Units;
import sun.jvmstat.monitor.VmIdentifier;

/**
 * JUnit rule that manages usage of {@link WithSystemProperty} annotations.<br/>
 * Can be used as both {@link Rule} and {@link ClassRule}.
 *
 * @see WithSystemProperty
 * @see Rule
 * @see ClassRule
 */
public class SystemPropertiesRule implements TestRule {
    /**
     * {@inheritDoc}
     *
     * @throws NoSuchMethodError If test method wasn't found for some reason.
     */
    @Override public Statement apply(Statement base, Description desc) {
        Class<?> testCls = desc.getTestClass();

        String testName = desc.getMethodName();

        if (testName == null)
            return classStatement(testCls, base);
        else
            return methodStatement(getTestMethod(testCls, testName), base);
    }

    /**
     * Searches for public method with no parameter by the class and methods name.
     *
     * @param testCls Class to search method in.
     * @param testName Method name.
     * @return Non-null method object.
     * @throws NoSuchMethodError If test method wasn't found.
     */
    private Method getTestMethod(Class<?> testCls, String testName) {
        // Remove custom parameters from "@Parameterized" test.
        int bracketIdx = testName.indexOf('[');

        String testMtdName = bracketIdx >= 0 ? testName.substring(0, bracketIdx) : testName;

        Method testMtd;
        try {
            testMtd = testCls.getMethod(testMtdName);

        }
        catch (NoSuchMethodException e) {
            throw new NoSuchMethodError(S.toString("Test method wasn't found",
                "testClass", testCls.getSimpleName(), false,
                "methodName", testName, false,
                "testMtdName", testMtdName, false
            ));
        }
        return testMtd;
    }

    /**
     * @return Statement that sets all required system properties before class and cleans them after.
     */
    private Statement classStatement(Class<?> testCls, Statement base) {
        return DelegatingJUnitStatement.wrap(() -> {
            List<T2<String, String>> clsSysProps = setSystemPropertiesBeforeClass(testCls);

            try {
                base.evaluate();
            }
            finally {
                clearSystemProperties(clsSysProps);
            }
        });
    }

    /**
     * @return Statement that sets all required system properties before test method and cleans them after.
     */
    private Statement methodStatement(Method testMtd, Statement base) {
        return DelegatingJUnitStatement.wrap(() -> {
            List<T2<String, String>> testSysProps = setSystemPropertiesBeforeTestMethod(testMtd);

            try {
                base.evaluate();
            }
            finally {
                clearSystemProperties(testSysProps);
            }
        });
    }

    /**
     * Set system properties before class.
     *
     * @param testCls Current test class.
     * @return List of updated properties in reversed order.
     */
    private List<T2<String, String>> setSystemPropertiesBeforeClass(Class<?> testCls) {
        List<WithSystemProperty[]> allProps = new ArrayList<>();

        for (Class<?> cls = testCls; cls != null; cls = cls.getSuperclass()) {
            SystemPropertiesList clsProps = cls.getAnnotation(SystemPropertiesList.class);

            if (clsProps != null)
                allProps.add(clsProps.value());
            else {
                WithSystemProperty clsProp = cls.getAnnotation(WithSystemProperty.class);

                if (clsProp != null)
                    allProps.add(new WithSystemProperty[] {clsProp});
            }
        }

        Collections.reverse(allProps);

        // List of system properties to set when all tests in class are finished.
        final List<T2<String, String>> clsSysProps = new ArrayList<>();

        for (WithSystemProperty[] props : allProps) {
            for (WithSystemProperty prop : props) {
                String oldVal = System.setProperty(prop.key(), prop.value());

                clsSysProps.add(new T2<>(prop.key(), oldVal));
            }
        }

        Collections.reverse(clsSysProps);

        return clsSysProps;
    }

    /**
     * Set system properties before test method.
     *
     * @param testMtd Current test method.
     * @return List of updated properties in reversed order.
     */
    public List<T2<String, String>> setSystemPropertiesBeforeTestMethod(Method testMtd) {
        WithSystemProperty[] allProps = null;

        SystemPropertiesList testProps = testMtd.getAnnotation(SystemPropertiesList.class);

        if (testProps != null)
            allProps = testProps.value();
        else {
            WithSystemProperty testProp = testMtd.getAnnotation(WithSystemProperty.class);

            if (testProp != null)
                allProps = new WithSystemProperty[] {testProp};
        }

        // List of system properties to set when test is finished.
        List<T2<String, String>> testSysProps = new ArrayList<>();

        if (allProps != null) {
            for (WithSystemProperty prop : allProps) {
                String oldVal = System.setProperty(prop.key(), prop.value());

                testSysProps.add(new T2<>(prop.key(), oldVal));
            }
        }

        Collections.reverse(testSysProps);

        return testSysProps;
    }

    /**
     * Return old values of updated properties.
     *
     * @param sysProps List previously returned by {@link #setSystemPropertiesBeforeClass(java.lang.Class)}
     *      or {@link #setSystemPropertiesBeforeTestMethod(Method)}.
     */
    private void clearSystemProperties(List<T2<String, String>> sysProps) {
        for (T2<String, String> t2 : sysProps) {
            if (t2.getValue() == null)
                System.clearProperty(t2.getKey());
            else
                System.setProperty(t2.getKey(), t2.getValue());
        }

        try {
            System.out.println("<!> jvmstat report");
            MonitoredVm vm = MonitoredHost.getMonitoredHost(new HostIdentifier("localhost")).getMonitoredVm(new VmIdentifier("//" + U.jvmPid()), 0);

            for (Monitor monitor : vm.findByPattern(".*")) {
                if (monitor.getUnits() == Units.BYTES && !monitor.getName().contains(".gc."))
                    System.out.println(monitor.getName() + " = " + monitor.getValue() + " " + monitor.getUnits());
            }

            MBeanServer mBeanSrv = ManagementFactory.getPlatformMBeanServer();

            System.out.printf(
                "Direct total = %s%nDirect used = %s%nMapped total = %s%nMapped used = %s%n",
                mBeanSrv.getAttribute(ObjectName.getInstance("java.nio:type=BufferPool,name=direct"), "TotalCapacity"),
                mBeanSrv.getAttribute(ObjectName.getInstance("java.nio:type=BufferPool,name=direct"), "MemoryUsed"),
                mBeanSrv.getAttribute(ObjectName.getInstance("java.nio:type=BufferPool,name=mapped"), "TotalCapacity"),
                mBeanSrv.getAttribute(ObjectName.getInstance("java.nio:type=BufferPool,name=mapped"), "MemoryUsed")
            );

//            exec("ps", "-o", "%mem,pid", "ax");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** */
    private void exec(String... cmd) {
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            InputStream is = p.getInputStream();

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            PrintWriter writer = new PrintWriter(out);

            int val;
            while ((val = is.read()) != -1)
                writer.print((char)val);

            writer.flush();

            String str = out.toString("UTF-8");

            System.out.println(str);

//            Comparator<String> cmp = Comparator.comparingDouble(s -> Double.parseDouble(s.substring(0, s.indexOf(' '))));
//            String res = Stream.of(str.split("\n"))
//                .skip(1)
//                .map(String::trim)
//                .sorted(cmp.reversed())
//                .limit(5)
//                .collect(Collectors.joining("\n"));
//            System.out.println(res);

            int exitCode = p.waitFor();
            System.out.println("Exited with " + exitCode);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
