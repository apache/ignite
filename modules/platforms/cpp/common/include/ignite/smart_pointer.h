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
 * Declares ignite::SmartPointer class.
 */

#ifndef _IGNITE_IGNITE_SMART_POINTER
#define _IGNITE_IGNITE_SMART_POINTER

#include <ignite/common/common.h>

namespace ignite
{
    template<typename T>
    class SmartPointer
    {
    public:
        /**
         * Destructor.
         */
        virtual ~SmartPointer()
        {
            // No-op.
        }

        virtual const T& Get() const = 0;

        virtual T& Get() = 0;

        virtual bool IsNull() const = 0;
    };

    /**
     * Smart pointer wrapper class.
     *
     * Note, this class does not implement any smart pointer functionality
     * itself, instead it wraps existing wide-spread smart pointer
     * implementations and provides unified interface for them.
     */
    template<typename P>
    class SmartPointerHolder : public SmartPointer<typename P::element_type>
    {
    public:
        typedef typename P::element_type ElementType;

        /**
         * Destructor.
         */
        virtual ~SmartPointerHolder()
        {
            // No-op.
        }

        /**
         * Default constructor.
         */
        SmartPointerHolder() :
            ptr()
        {
            // No-op.
        }

        SmartPointerHolder(P ptr) :
            ptr(std::move(ptr))
        {
            // No-op.
        }

        const ElementType& Get() const
        {
            return *ptr;
        }

        ElementType& Get()
        {
            return *ptr;
        }

        bool IsNull() const
        {
            return 0 == ptr.get();
        }

    private:
        P ptr;
    };
}

#endif //_IGNITE_IGNITE_SMART_POINTER