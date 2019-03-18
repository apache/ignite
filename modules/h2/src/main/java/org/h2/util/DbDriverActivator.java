/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

/**
 * The driver activator loads the H2 driver when starting the bundle. The driver
 * is unloaded when stopping the bundle.
 */
public class DbDriverActivator implements BundleActivator {

    private static final String DATASOURCE_FACTORY_CLASS =
            "org.osgi.service.jdbc.DataSourceFactory";

    /**
     * Start the bundle. If the 'org.osgi.service.jdbc.DataSourceFactory' class
     * is available in the class path, this will load the database driver and
     * register the DataSourceFactory service.
     *
     * @param bundleContext the bundle context
     */
    @Override
    public void start(BundleContext bundleContext) {
        org.h2.Driver driver = org.h2.Driver.load();
        try {
            JdbcUtils.loadUserClass(DATASOURCE_FACTORY_CLASS);
        } catch (Exception e) {
            // class not found - don't register
            return;
        }
        // but don't ignore exceptions in this call
        OsgiDataSourceFactory.registerService(bundleContext, driver);
    }

    /**
     * Stop the bundle. This will unload the database driver. The
     * DataSourceFactory service is implicitly un-registered by the OSGi
     * framework.
     *
     * @param bundleContext the bundle context
     */
    @Override
    public void stop(BundleContext bundleContext) {
        org.h2.Driver.unload();
    }

}
