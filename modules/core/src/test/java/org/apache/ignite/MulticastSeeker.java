package org.apache.ignite;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.reflections.Reflections;

public class MulticastSeeker {
    private final Reflections reflections = new Reflections("org.apache.ignite");

    public static void main(String[] args) {
        new MulticastSeeker().doMain();
    }

    private void doMain() {
        Set<String> multiTestNames = collectMulticastTests();

        List<Class<? extends TestSuite>> suites =
            new ArrayList<>(reflections.getSubTypesOf(TestSuite.class));

        suites.sort(Comparator.comparing(Class::getName));

        for (Class<? extends TestSuite> suiteCls : suites) {
            List<String> suiteMultiTests = new ArrayList<>();

            try {
                TestSuite suite = invokeMethod(suiteCls, null, "suite");

                for (int i = 0; i < suite.testCount(); i++) {
                    Test test = suite.testAt(i);

                    if (test instanceof TestSuite) {
                        TestSuite childSuite = (TestSuite)test;

                        if (multiTestNames.contains(childSuite.getName()))
                            suiteMultiTests.add(childSuite.getName());
                    }
                    else
                        System.err.println(suite.getName() + "[" + i + "] is not a TestSuite");
                }

                Collections.sort(suiteMultiTests);

                System.out.println(suiteCls.getName() + ": " + suiteMultiTests.size() + " tests");

                for (String testName : suiteMultiTests)
                    System.out.println("\t" + testName);

                System.out.println();
            }
            catch (ReflectiveOperationException e) {
                e.printStackTrace();
            }
        }
    }

    private Set<String> collectMulticastTests() {
        Set<Class<? extends GridAbstractTest>> testClasses = reflections.getSubTypesOf(GridAbstractTest.class);

        Set<String> names = new HashSet<>();

        for (Class<? extends GridAbstractTest> cls : testClasses) {
            if (!Modifier.isAbstract(cls.getModifiers()) && isMulticast(cls))
                names.add(cls.getName());
        }

        return names;
    }

    private boolean isMulticast(Class<? extends GridAbstractTest> cls) {
        try {
            Constructor<? extends GridAbstractTest> ctor = cls.getConstructor();

            GridAbstractTest testInstance = ctor.newInstance();

            IgniteConfiguration igniteCfg =
                invokeMethod(cls, testInstance, "getConfiguration", "server");

            DiscoverySpi disco = igniteCfg.getDiscoverySpi();
            if (disco instanceof TcpDiscoverySpi)
                return ((TcpDiscoverySpi)disco).getIpFinder() instanceof TcpDiscoveryMulticastIpFinder;
            else
                return false;
        }
        catch (Exception e) {
            return false;
        }
    }

    private <T> T invokeMethod(Class<?> cls, Object instance, String mtdName, Object... params) throws ReflectiveOperationException {
        Class<?>[] paramClasses = getParamClasses(params);

        Method getCfgMtd = findMethod(cls, mtdName, paramClasses);

        getCfgMtd.setAccessible(true);

        return (T)getCfgMtd.invoke(instance, params);
    }

    private Class<?>[] getParamClasses(Object[] params) {
        Class<?>[] classes = new Class[params.length];

        for (int i = 0; i < params.length; i++)
            classes[i] = params[i].getClass();

        return classes;
    }

    private Method findMethod(Class<?> cls, String mtdName, Class<?>... params) throws NoSuchMethodException {
        Class<?> curCls = cls;

        while (curCls != null) {
            try {
                return curCls.getDeclaredMethod(mtdName, params);
            }
            catch (NoSuchMethodException ignore) {
            }

            curCls = curCls.getSuperclass();
        }

        throw new NoSuchMethodException(cls.getName() + "." + mtdName);
    }
}
