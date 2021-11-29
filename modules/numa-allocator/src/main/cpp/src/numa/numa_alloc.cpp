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
        BitSetImpl() : size_(numa_max_node() + 1) {
            mask_ = numa_bitmask_alloc(size_);
        }

        BitSetImpl(const BitSetImpl &other) : size_(other.size_) {
            mask_ = numa_bitmask_alloc(size_);
            copy_bitmask_to_bitmask(other.mask_, mask_);
        }

        void SetBit(size_t idx, bool val) {
            if (idx < size_) {
                if (val)
                    numa_bitmask_setbit(mask_, idx);
                else
                    numa_bitmask_clearbit(mask_, idx);
            }
        }

        void SetAll(bool val) {
            if (val)
                numa_bitmask_setall(mask_);
            else
                numa_bitmask_clearall(mask_);
        }

        bool GetBit(size_t idx) const {
            if (idx < size_)
                return numa_bitmask_isbitset(mask_, idx);
            else
                return false;
        }

        bool Equals(const BitSetImpl &other) const {
            return numa_bitmask_equal(mask_, other.mask_);
        }

        size_t Size() const {
            return size_;
        }

        bitmask *Raw() {
            return mask_;
        }

        ~BitSetImpl() {
            numa_bitmask_free(mask_);
        }

    private:
        bitmask *mask_;
        size_t size_;
    };

    BitSet::BitSet() {
        p_impl_ = new BitSetImpl();
    }

    BitSet::BitSet(const BitSet &other) {
        p_impl_ = new BitSetImpl(*other.p_impl_);
    }

    BitSet &BitSet::operator=(const BitSet &other) {
        if (this != &other) {
            BitSet tmp(other);
            std::swap(this->p_impl_, tmp.p_impl_);
        }
        return *this;
    }

    bool BitSet::Get(size_t pos) const {
        return p_impl_->GetBit(pos);
    }

    void BitSet::Set(size_t pos, bool value) {
        p_impl_->SetBit(pos, value);
    }

    void BitSet::Set() {
        p_impl_->SetAll(true);
    }

    void BitSet::Reset(size_t pos) {
        p_impl_->SetBit(pos, false);
    }

    void BitSet::Reset() {
        p_impl_->SetAll(false);
    }

    size_t BitSet::Size() const {
        return p_impl_->Size();
    }

    bool BitSet::operator==(const BitSet &other) {
        return this->p_impl_->Equals(*other.p_impl_);
    }

    bool BitSet::operator!=(const BitSet &other) {
        return !(*this == other);
    }

    BitSet::~BitSet() {
        delete p_impl_;
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

    int NumaNodesCount() {
        return numa_max_node() + 1;
    }

    /**
     *  Memory layout:
     *  +-------------------------------+------------------------+
     *  | Header (sizeof(max_align_t))  |  Application memory    |
     *  +------------------------------ +------------------------+
     *                                  ^
     *                                  |
     *                          Result pointer
     * Size of application memory chunk is written to header.
     * Total allocated size equals to size of application memory chunk plus sizeof(max_align_t).
     */
    union region_size {
        size_t size;
        max_align_t a;
    };

    template<typename Func, typename ...Args>
    inline void *NumaAllocHelper(Func f, size_t size, Args ...args) {
        auto ptr = static_cast<region_size *>(f(size + sizeof(region_size), args...));
        if (ptr) {
            ptr->size = size;
            ptr++;
        }
        return ptr;
    }

    inline region_size* ConvertPointer(void* buf) {
        if (buf) {
            auto *ptr = static_cast<region_size *>(buf);
            ptr--;
            return ptr;
        }
        return nullptr;
    }

    void *Alloc(size_t size) {
        return NumaAllocHelper(numa_alloc, size);
    }

    void *Alloc(size_t size, int node) {
        return NumaAllocHelper(numa_alloc_onnode, size, node);
    }

    void *AllocLocal(size_t size) {
        return NumaAllocHelper(numa_alloc_local, size);
    }

    void *AllocInterleaved(size_t size) {
        return NumaAllocHelper(numa_alloc_interleaved, size);
    }

    void *AllocInterleaved(size_t size, const BitSet &node_set) {
        return NumaAllocHelper(numa_alloc_interleaved_subset, size, node_set.p_impl_->Raw());
    }

    size_t Size(void *buf) {
        auto ptr = ConvertPointer(buf);
        if (ptr) {
            return ptr->size;
        }
        return 0;
    }

    void Free(void *buf) {
        auto ptr = ConvertPointer(buf);
        if (ptr) {
            numa_free(ptr, ptr->size + sizeof(region_size));
        }
    }
}
