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

#include <numa.h>
#include <numa/numa_alloc.h>

namespace numa {
    class BitSet::BitSetImpl {
        public:
            BitSetImpl(): size(numa_max_node() + 1) {
                mask = numa_bitmask_alloc(size);
            }

            BitSetImpl(const BitSetImpl& other): size(other.size) {
                mask = numa_bitmask_alloc(size);
                copy_bitmask_to_bitmask(other.mask, mask);
            }

            void SetBit(size_t idx, bool val) {
                if (val)
                    numa_bitmask_setbit(mask, idx);
                else
                    numa_bitmask_clearbit(mask, idx);
            }

            void SetAll(bool val) {
                if (val)
                    numa_bitmask_setall(mask);
                else
                    numa_bitmask_clearall(mask);
            }

            bool GetBit(size_t idx) const {
                return numa_bitmask_isbitset(mask, idx);
            }

            bool Equals(const BitSetImpl& other) const {
                return numa_bitmask_equal(mask, other.mask);
            }

            size_t Size() const {
                return size;
            }

            bitmask* Raw() {
                return mask;
            }

            ~BitSetImpl() {
                numa_bitmask_free(mask);
            }
        private:
            bitmask* mask;
            size_t size;
    };

    BitSet::BitSet() {
        pImpl = new BitSetImpl();
    }

    BitSet::BitSet(const BitSet &other) {
        pImpl = new BitSetImpl(*other.pImpl);
    }

    BitSet& BitSet::operator=(const BitSet &other) {
        if (this != &other) {
            BitSet tmp(other);
            std::swap(this->pImpl, tmp.pImpl);
        }
        return *this;
    }

    bool BitSet::Get(size_t pos) const {
        return pImpl->GetBit(pos);
    }

    void BitSet::Set(size_t pos, bool value) {
        pImpl->SetBit(pos, value);
    }

    void BitSet::Set() {
        pImpl->SetAll(true);
    }

    void BitSet::Reset(size_t pos) {
        pImpl->SetBit(pos, false);
    }

    void BitSet::Reset() {
        pImpl->SetAll(false);
    }

    size_t BitSet::Size() const {
        return pImpl->Size();
    }

    bool BitSet::operator==(const BitSet &other) {
        return this->pImpl->Equals(*other.pImpl);
    }

    bool BitSet::operator!=(const BitSet &other) {
        return !(*this == other);
    }

    BitSet::~BitSet() {
        delete pImpl;
    }

    std::ostream &operator<<(std::ostream &os, const BitSet &set) {
        os << '{';
        for (size_t i = 0; i < set.Size(); ++i) {
            os << set[i];
            if (i < set.Size() - 1) {
                os << ", ";
            }
        }
        os << '}';
        return os;
    }

    union region_size {
        size_t size;
        max_align_t a;
    };

#define POSTPROC_PTR(Ptr, Size) do { \
    if (Ptr) { \
        (Ptr)->size = (Size) - sizeof(region_size); \
        (Ptr)++; \
    } \
} while(0)

#define ALLOC_PTR(Ptr, Func, Size) { \
    (Ptr) = static_cast<region_size*>(Func((Size) + sizeof(region_size))); \
    POSTPROC_PTR(Ptr, Size); \
}

#define ALLOC_PTR_PARAM(Ptr, Func, Size, ...) { \
    (Ptr) = static_cast<region_size*>(Func((Size) + sizeof(region_size), __VA_ARGS__)); \
    POSTPROC_PTR(Ptr, Size); \
}

    void* Alloc(size_t size) {
        region_size* ptr;
        ALLOC_PTR(ptr, numa_alloc, size)
        return ptr;
    }

    void* Alloc(size_t size, int node) {
        region_size* ptr;
        ALLOC_PTR_PARAM(ptr, numa_alloc_onnode, size, node)
        return ptr;
    }

    void* AllocLocal(size_t size) {
        region_size* ptr;
        ALLOC_PTR(ptr, numa_alloc_local, size)
        return ptr;
    }

    void* AllocInterleaved(size_t size) {
        region_size* ptr;
        ALLOC_PTR(ptr, numa_alloc_interleaved, size)
        return ptr;
    }

    void* AllocInterleaved(size_t size, const BitSet& node_set) {
        region_size* ptr;
        ALLOC_PTR_PARAM(ptr, numa_alloc_interleaved_subset, size, node_set.pImpl->Raw())
        return ptr;
    }

    size_t Size(void *buf) {
        if (buf) {
            auto *ptr = static_cast<region_size*>(buf);
            ptr--;
            return ptr->size;
        }
        return 0;
    }

    void Free(void *buf) {
        if (buf) {
            auto *ptr = static_cast<region_size*>(buf);
            ptr--;
            numa_free(ptr, ptr->size + sizeof(region_size));
        }
    }
}
