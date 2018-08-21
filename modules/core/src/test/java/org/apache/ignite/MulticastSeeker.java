package org.apache.ignite;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.reflections.Reflections;

public class MulticastSeeker {
    public static void main(String[] args) {
        Reflections reflections = new Reflections("org.apache.ignite");

        Set<Class<? extends GridAbstractTest>> testClasses = reflections.getSubTypesOf(GridAbstractTest.class);

        for (Class<? extends GridAbstractTest> cls : testClasses) {
            if (!Modifier.isAbstract(cls.getModifiers()) && isMulticast(cls))
                System.out.println(cls.getName());
        }
    }

    private static boolean isMulticast(Class<? extends GridAbstractTest> cls) {
        try {
            Constructor<? extends GridAbstractTest> ctor = cls.getConstructor();

            GridAbstractTest testInstance = ctor.newInstance();

            IgniteConfiguration igniteCfg = invokeMethod(testInstance, "getConfiguration", "server");

            DiscoverySpi disco = igniteCfg.getDiscoverySpi();
            if (disco instanceof TcpDiscoverySpi)
                return ((TcpDiscoverySpi)disco).getIpFinder() instanceof TcpDiscoveryMulticastIpFinder;
            else
                return false;
        }
        catch (Exception e) {
            // e.printStackTrace();
            return false;
        }
    }

    private static <T> T invokeMethod(Object instance, String mtdName, Object... params) throws ReflectiveOperationException {
        Class<?> cls = instance.getClass();

        Class<?>[] paramClasses = getParamClasses(params);

        Method getCfgMtd = findMethod(cls, mtdName, paramClasses);

        getCfgMtd.setAccessible(true);

        return (T)getCfgMtd.invoke(instance, params);
    }

    private static Class<?>[] getParamClasses(Object[] params) {
        Class<?>[] classes = new Class[params.length];

        for (int i = 0; i < params.length; i++)
            classes[i] = params[i].getClass();

        return classes;
    }

    private static Method findMethod(Class<?> cls, String mtdName, Class<?>... params) throws NoSuchMethodException {
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
