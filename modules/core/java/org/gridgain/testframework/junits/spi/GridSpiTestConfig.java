// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework.junits.spi;

import java.lang.annotation.*;

/**
 * Annotates a getter method value of which is used to configure implementation SPI.
 *
 * @author @java.author
 * @version @java.version
 */
@SuppressWarnings({"JavaDoc"})
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface GridSpiTestConfig {
    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public enum ConfigType {
        /** */
        SELF,

        /** */
        DISCOVERY,

        /** */
        BOTH
    }

    /** */
    @SuppressWarnings({"JavaDoc"}) ConfigType type() default ConfigType.SELF;

    /** */
    String setterName() default "";
}
