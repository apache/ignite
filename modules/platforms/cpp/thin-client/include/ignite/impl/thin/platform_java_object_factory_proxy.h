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

#ifndef _IGNITE_IMPL_THIN_PLATFORM_JAVA_OBJECT_FACTORY_PROXY
#define _IGNITE_IMPL_THIN_PLATFORM_JAVA_OBJECT_FACTORY_PROXY

#include <string>
#include <map>

#include <ignite/common/common.h>
#include <ignite/binary/binary_type.h>
#include <ignite/binary/binary_writer.h>

#include <ignite/impl/binary/binary_utils.h>
#include <ignite/impl/thin/copyable_writable.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Represents the factory type.
             */
            struct FactoryType
            {
                enum Type
                {
                    USER = 0,

                    DEFAULT = 1,
                };
            };

            /**
             * Maps to PlatformJavaObjectFactoryProxy in Java.
             */
            class PlatformJavaObjectFactoryProxy
            {
                friend struct ignite::binary::BinaryType<impl::thin::PlatformJavaObjectFactoryProxy>;
            public:
                /**
                 * Default constructor.
                 */
                PlatformJavaObjectFactoryProxy()
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param factoryType Factory type.
                 * @param factoryClassName Name of the Java factory class.
                 */
                PlatformJavaObjectFactoryProxy(FactoryType::Type factoryType, const std::string& factoryClassName) :
                    factoryType(factoryType),
                    factoryClassName(factoryClassName),
                    properties()
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~PlatformJavaObjectFactoryProxy()
                {
                    ClearProperties();
                }

                /**
                 * Get Java factory class name.
                 *
                 * @return Java factory class name.
                 */
                const std::string& GetFactoryClassName() const
                {
                    return factoryClassName;
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
                    std::map<std::string, Writable*>::iterator it = properties.find(name);
                    std::auto_ptr< CopyableWritableImpl<T> > newProp(new CopyableWritableImpl<T>(value));
                    if (it != properties.end())
                    {
                        delete it->second;
                        it->second = newProp.release();
                    }
                    else
                        properties[name] = newProp.release();
                }

                /**
                 * Clear set properties.
                 * Set properties to empty set.
                 */
                void ClearProperties()
                {
                    std::map<std::string, Writable*>::iterator it;
                    for (it = properties.begin(); it != properties.end(); ++it)
                        delete it->second;

                    properties.clear();
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(PlatformJavaObjectFactoryProxy);

                /** Type of the factory. */
                FactoryType::Type factoryType;

                /** Name of the Java factory class. */
                std::string factoryClassName;

                /** The properties. */
                std::map<std::string, Writable*> properties;
            };

            typedef common::concurrent::SharedPointer<PlatformJavaObjectFactoryProxy> SP_PlatformJavaObjectFactoryProxy;
        }
    }

    namespace binary
    {
        template<>
        struct IGNITE_IMPORT_EXPORT BinaryType<impl::thin::PlatformJavaObjectFactoryProxy> :
            BinaryTypeDefaultAll<impl::thin::PlatformJavaObjectFactoryProxy>
        {
            /**
             * Get binary object type ID.
             *
             * @return Type ID.
             */
            static int32_t GetTypeId()
            {
                enum { PLATFORM_JAVA_OBJECT_FACTORY_PROXY = 99 };

                return PLATFORM_JAVA_OBJECT_FACTORY_PROXY;
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "PlatformJavaObjectFactoryProxy";
            }

            static void Write(BinaryWriter& writer, const impl::thin::PlatformJavaObjectFactoryProxy& val)
            {
                ignite::binary::BinaryRawWriter rawWriter(writer.RawWriter());

                rawWriter.WriteInt32(val.factoryType);
                rawWriter.WriteString(val.factoryClassName);
                rawWriter.WriteNull(); // Payload is not implemented for now

                if (val.properties.empty())
                {
                    rawWriter.WriteInt32(0);
                    return;
                }

                rawWriter.WriteInt32(static_cast<int32_t>(val.properties.size()));

                std::map<std::string, impl::thin::Writable*>::const_iterator it;
                for (it = val.properties.begin(); it != val.properties.end(); ++it)
                {
                    rawWriter.WriteString(it->first);
                    it->second->Write(impl::binary::BinaryUtils::ImplFromFacade(rawWriter));
                }
            }

            static void Read(BinaryReader&, impl::thin::PlatformJavaObjectFactoryProxy&)
            {
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Should never be deserialized on C++ side");
            }
        };
    }
}

#endif //_IGNITE_IMPL_THIN_PLATFORM_JAVA_OBJECT_FACTORY_PROXY
