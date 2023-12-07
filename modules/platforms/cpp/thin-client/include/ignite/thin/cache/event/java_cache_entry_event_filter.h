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

/**
 * @file
 * Declares ignite::thin::cache::event::JavaCacheEntryEventFilter class.
 */

#ifndef _IGNITE_THIN_CACHE_EVENT_JAVA_CACHE_ENTRY_EVENT_FILTER
#define _IGNITE_THIN_CACHE_EVENT_JAVA_CACHE_ENTRY_EVENT_FILTER

#include <ignite/thin/cache/event/cache_entry_event.h>

#include <ignite/impl/thin/platform_java_object_factory_proxy.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace cache
            {
                class CacheClientImpl;
            }
        }
    }

    namespace thin
    {
        namespace cache
        {
            namespace event
            {
                /**
                 * Java cache entry event filter.
                 *
                 * All templated types should be default-constructable,
                 * copy-constructable and assignable.
                 */
                class JavaCacheEntryEventFilter
                {
                    friend class ignite::impl::thin::cache::CacheClientImpl;
                public:
                    /**
                     * Default constructor.
                     */
                    JavaCacheEntryEventFilter()
                    {
                        // No-op.
                    }

                    /**
                     * Constructor.
                     *
                     * @param factoryClassName Name of the Java factory class.
                     */
                    JavaCacheEntryEventFilter(const std::string& factoryClassName) :
                        factory(new impl::thin::PlatformJavaObjectFactoryProxy(
                            impl::thin::FactoryType::USER, factoryClassName))
                    {
                        // No-op.
                    }

                    /**
                     * Destructor.
                     */
                    virtual ~JavaCacheEntryEventFilter()
                    {
                        // No-op.
                    }

                    /**
                     * Get Java remote filter factory class name.
                     *
                     * @return Java factory class name.
                     */
                    const std::string& GetFactoryClassName() const
                    {
                        return factory.Get()->GetFactoryClassName();
                    }

                    /**
                     * Add property.
                     *
                     * Template argument type should be copy-constructable and assignable. Also BinaryType class template
                     * should be specialized for this type.
                     *
                     * @param name Property name.
                     * @param value Property value.
                     */
                    template<typename T>
                    void SetProperty(const std::string& name, const T& value)
                    {
                        factory.Get()->template SetProperty<T>(name, value);
                    }

                    /**
                     * Clear set properties.
                     * Set properties for the filter to empty set.
                     */
                    void ClearProperties()
                    {
                        factory.Get()->ClearProperties();
                    }

                private:
                    /** Java object factory proxy. */
                    impl::thin::SP_PlatformJavaObjectFactoryProxy factory;
                };
            }
        }
    }
}

#endif //_IGNITE_THIN_CACHE_EVENT_JAVA_CACHE_ENTRY_EVENT_FILTER
