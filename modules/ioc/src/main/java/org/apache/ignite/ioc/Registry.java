package org.apache.ignite.ioc;

import org.apache.ignite.IgniteCheckedException;

/**
 * Facade to other dependency injection mechanisms and bean registries.
 *
 * Purpose of this type is detachment of actual lookup from ignite specific SPI.
 * Both operations can be called by Ignite itself to initialize fields annotated with
 * {@link org.apache.ignite.resources.InjectResource}.
 *
 * @author ldywicki
 */
public interface Registry {

  <T> T lookup(Class<T> type);
  Object lookup(String name);
  Object unwrapTarget(Object target) throws IgniteCheckedException;

}
