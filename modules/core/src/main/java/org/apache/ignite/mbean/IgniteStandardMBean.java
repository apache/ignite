/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.mbean;

import org.gridgain.grid.util.typedef.internal.*;
import javax.management.*;
import java.lang.reflect.*;
import java.util.*;

/**
 * Extension of standard Java MBean. Overrides some hooks to return
 * annotation based descriptions.
 */
public class IgniteStandardMBean extends StandardMBean {
    /**
     * Objects maps from primitive classes to primitive object classes.
     */
    private static final Map<String, Class<?>> primCls = new HashMap<>();

    /**
     * Static constructor.
     */
    static{
        primCls.put(Boolean.TYPE.toString().toLowerCase(), Boolean.TYPE);
        primCls.put(Character.TYPE.toString().toLowerCase(), Character.TYPE);
        primCls.put(Byte.TYPE.toString().toLowerCase(), Byte.TYPE);
        primCls.put(Short.TYPE.toString().toLowerCase(), Short.TYPE);
        primCls.put(Integer.TYPE.toString().toLowerCase(), Integer.TYPE);
        primCls.put(Long.TYPE.toString().toLowerCase(), Long.TYPE);
        primCls.put(Float.TYPE.toString().toLowerCase(), Float.TYPE);
        primCls.put(Double.TYPE.toString().toLowerCase(), Double.TYPE);
    }

    /**
     * Make a DynamicMBean out of the object implementation, using the specified
     * mbeanInterface class.
     *
     * @param implementation The implementation of this MBean.
     * @param mbeanInterface The Management Interface exported by this
     *      MBean's implementation. If {@code null}, then this
     *      object will use standard JMX design pattern to determine
     *      the management interface associated with the given
     *      implementation.
     *      If {@code null} value passed then information will be built by
     *      {@link StandardMBean}
     *
     * @exception NotCompliantMBeanException if the {@code mbeanInterface}
     *    does not follow JMX design patterns for Management Interfaces, or
     *    if the given {@code implementation} does not implement the
     *    specified interface.
     */
    public <T> IgniteStandardMBean(T implementation, Class<T> mbeanInterface)
        throws NotCompliantMBeanException {
        super(implementation, mbeanInterface);
    }

    /** {@inheritDoc} */
    @Override protected String getDescription(MBeanAttributeInfo info) {
        String str = super.getDescription(info);

        String methodName = (info.isIs() ? "is" : "get") + info.getName();

        try {
            // Recursively get method.
            Method mtd = findMethod(getMBeanInterface(), methodName, new Class[]{});

            if (mtd != null) {
                IgniteMBeanDescription desc = mtd.getAnnotation(IgniteMBeanDescription.class);

                if (desc != null) {
                    str = desc.value();

                    assert str != null : "Failed to find method: " + mtd;
                    assert str.trim().length() > 0 : "Method description cannot be empty: " + mtd;

                    // Enforce proper English.
                    assert Character.isUpperCase(str.charAt(0)) == true :
                        "Description must start with upper case: " + str;

                    assert str.charAt(str.length() - 1) == '.' : "Description must end with period: " + str;
                }
            }
        }
        catch (SecurityException e) {
            // No-op. Default value will be returned.
        }

        return str;
    }

    /** {@inheritDoc} */
    @Override protected String getDescription(MBeanInfo info) {
        String str = super.getDescription(info);

        // Return either default one or given by annotation.
        IgniteMBeanDescription desc = U.getAnnotation(getMBeanInterface(), IgniteMBeanDescription.class);

        if (desc != null) {
            str = desc.value();

            assert str != null;
            assert str.trim().length() > 0;

            // Enforce proper English.
            assert Character.isUpperCase(str.charAt(0)) == true : str;
            assert str.charAt(str.length() - 1) == '.' : str;
        }

        return str;
    }

    /** {@inheritDoc} */
    @Override protected String getDescription(MBeanOperationInfo info) {
        String str = super.getDescription(info);

        try {
            Method m = getMethod(info);

            IgniteMBeanDescription desc = m.getAnnotation(IgniteMBeanDescription.class);

            if (desc != null) {
                str = desc.value();

                assert str != null;
                assert str.trim().length() > 0;

                // Enforce proper English.
                assert Character.isUpperCase(str.charAt(0)) == true : str;
                assert str.charAt(str.length() - 1) == '.' : str;
            }
        }
        catch (SecurityException | ClassNotFoundException e) {
            // No-op. Default value will be returned.
        }

        return str;
    }

    /** {@inheritDoc} */
    @Override protected String getDescription(MBeanOperationInfo op, MBeanParameterInfo param, int seq) {
        String str = super.getDescription(op, param, seq);

        try {
            Method m = getMethod(op);

            IgniteMBeanParametersDescriptions decsAnn = m.getAnnotation(IgniteMBeanParametersDescriptions.class);

            if (decsAnn != null) {
                assert decsAnn.value() != null;
                assert seq < decsAnn.value().length;

                str = decsAnn.value()[seq];

                assert str != null;
                assert str.trim().length() > 0;

                // Enforce proper English.
                assert Character.isUpperCase(str.charAt(0)) == true : str;
                assert str.charAt(str.length() - 1) == '.' : str;
            }
        }
        catch (SecurityException | ClassNotFoundException e) {
            // No-op. Default value will be returned.
        }

        return str;
    }

    /** {@inheritDoc} */
    @Override protected String getParameterName(MBeanOperationInfo op, MBeanParameterInfo param, int seq) {
        String str = super.getParameterName(op, param, seq);

        try {
            Method m = getMethod(op);

            IgniteMBeanParametersNames namesAnn = m.getAnnotation(IgniteMBeanParametersNames.class);

            if (namesAnn != null) {
                assert namesAnn.value() != null;
                assert seq < namesAnn.value().length;

                str = namesAnn.value()[seq];

                assert str != null;
                assert str.trim().length() > 0;
            }
        }
        catch (SecurityException | ClassNotFoundException e) {
            // No-op. Default value will be returned.
        }

        return str;
    }

    /**
     * Gets method by operation info.
     *
     * @param op MBean operation info.
     * @return Method.
     * @throws ClassNotFoundException Thrown if parameter type is unknown.
     * @throws SecurityException Thrown if method access is not allowed.
     */
    private Method getMethod(MBeanOperationInfo op) throws ClassNotFoundException, SecurityException {
        String methodName = op.getName();

        MBeanParameterInfo[] signature = op.getSignature();

        Class<?>[] params = new Class<?>[signature.length];

        for (int i = 0; i < signature.length; i++) {
            // Parameter type is either a primitive type or class. Try both.
            Class<?> type = primCls.get(signature[i].getType().toLowerCase());

            if (type == null)
                type = Class.forName(signature[i].getType());

            params[i] = type;
        }

        return findMethod(getMBeanInterface(), methodName, params);
    }

    /**
     * Finds method for the given interface.
     *
     * @param itf MBean interface.
     * @param methodName Method name.
     * @param params Method parameters.
     * @return Method.
     */
    @SuppressWarnings("unchecked")
    private Method findMethod(Class itf, String methodName, Class[] params) {
        assert itf.isInterface() == true;

        Method res = null;

        // Try to get method from given interface.
        try {
            res = itf.getDeclaredMethod(methodName, params);

            if (res != null)
                return res;
        }
        catch (NoSuchMethodException e) {
            // No-op. Default value will be returned.
        }

        // Process recursively super interfaces.
        Class[] superItfs = itf.getInterfaces();

        for (Class superItf: superItfs) {
            res = findMethod(superItf, methodName, params);

            if (res != null)
                return res;
        }

        return res;
    }
}
