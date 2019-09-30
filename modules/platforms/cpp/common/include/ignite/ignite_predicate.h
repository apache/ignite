/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 /**
  * @file
  * Declares ignite::IgnitePredicate class.
  */

#ifndef _IGNITE_IGNITE_PREDICATE
#define _IGNITE_IGNITE_PREDICATE

namespace ignite
{
    /**
     * IgnitePredicate base class.
     *
     * User should inherit from it to implement own predicate types.
     */
    template<typename T>
    class IGNITE_IMPORT_EXPORT IgnitePredicate
    {
    public:
        virtual bool operator()(T&) = 0;

        /**
         * Destructor.
         */
        virtual ~IgnitePredicate()
        {
            // No-op.
        }
    };
}

#endif //_IGNITE_IGNITE_PREDICATE