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

#ifndef _IGNITE_IMPL_INTEROP_INTEROP_STREAM_POSITION_GUARD
#define _IGNITE_IMPL_INTEROP_INTEROP_STREAM_POSITION_GUARD

#include "ignite/impl/interop/interop_memory.h"

namespace ignite
{
    namespace impl
    {
        namespace interop
        {
            /**
             * Interop stream position guard.
             */
            template<typename T>
            class IGNITE_IMPORT_EXPORT InteropStreamPositionGuard {
            public:
                /**
                 * Create new position guard and saves current stream position.
                 *
                 * @param stream Stream which position should be saved.
                 */
                InteropStreamPositionGuard(T& stream) : stream(&stream), pos(stream.Position())
                {
                    //No-op
                }

                /**
                 * Destructor.
                 *
                 * Restores stream's position to a saved one on destruction.
                 */
                ~InteropStreamPositionGuard()
                {
                    if (stream)
                        stream->Position(pos);
                }

                /**
                 * Releases guard so it will not restore streams position on destruction.
                 *
                 * @param val Value.
                 */
                void Release()
                {
                    stream = 0;
                }

            private:
                /** Stream. */
                T* stream;

                /** Saved position. */
                int32_t pos;

                IGNITE_NO_COPY_ASSIGNMENT(InteropStreamPositionGuard)
            };
        }
    }
}

#endif //_IGNITE_IMPL_INTEROP_INTEROP_STREAM_POSITION_GUARD
