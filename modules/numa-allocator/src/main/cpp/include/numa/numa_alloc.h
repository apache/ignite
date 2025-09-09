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

#ifndef _NUMA_ALLOC_H
#define _NUMA_ALLOC_H

#include <ostream>

namespace numa {
    class BitSet {
    public:
        BitSet();

        BitSet(const BitSet &other);

        BitSet &operator=(const BitSet &other);

        ~BitSet();

        class Reference {
        public:
            explicit Reference(BitSet &bitset, size_t pos) : bitset(&bitset), pos(pos) {}

            Reference &operator=(bool value) {
                bitset->Set(pos, value);
                return *this;
            }

            Reference &operator=(const Reference &other) {
                if (&other != this) {
                    bitset->Set(pos, other.bitset->Get(other.pos));
                }
                return *this;
            }

            explicit operator bool() const {
                return bitset->Get(pos);
            }

        private:
            BitSet *bitset;
            size_t pos;
        };

        Reference operator[](size_t pos) {
            return Reference(*this, pos);
        }

        bool operator[](size_t pos) const {
            return this->Get(pos);
        }

        bool operator==(const BitSet &other);

        bool operator!=(const BitSet &other);

        friend void *AllocInterleaved(size_t size, const BitSet &node_set);

        void Set(size_t pos, bool value = true);

        void Set();

        void Reset(size_t pos);

        void Reset();

        size_t Size() const;

        friend std::ostream &operator<<(std::ostream &os, const BitSet &set);

    private:
        bool Get(size_t pos) const;

        class BitSetImpl;

        BitSetImpl *p_impl_;
    };

    int NumaNodesCount();

    void *Alloc(size_t size);

    void *Alloc(size_t size, int node);

    void *AllocLocal(size_t size);

    void *AllocInterleaved(size_t size);

    void *AllocInterleaved(size_t size, const BitSet &node_set);

    size_t Size(void *ptr);

    void Free(void *ptr);
}

#endif //_NUMA_ALLOC_H
