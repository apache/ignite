package org.apache.ignite.springdata.misc;

import org.springframework.beans.factory.annotation.Value;

/**
 * Advanced SpEl Expressions into projection
 *
 * @author Manuel Núñez Sánchez (manuel.nunez@hawkore.com)
 */
public interface PersonProjection {

    String getFirstName();

    /**
     * Sample of using registered spring bean into SpEL expression
     * @return
     */
    @Value("#{@sampleExtensionBean.transformParam(target.firstName)}")
    String getFirstNameTransformed();

    /**
     * Sample of using SpEL expression
     * @return
     */
    @Value("#{target.firstName + ' ' + target.secondName}")
    String getFullName();
}
