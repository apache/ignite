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

#include <vector>
#include <map>
#include <algorithm>

#include <ignite/common/concurrent.h>

#include "ignite/impl/binary/binary_type_manager.h"

using namespace ignite::common::concurrent;

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            BinaryTypeManager::BinaryTypeManager() :
                snapshots(new std::map<int32_t, SPSnap>),
                pending(new std::vector<SPSnap>),
                cs(),
                updater(0),
                pendingVer(0),
                ver(0)
            {
                // No-op.
            }

            BinaryTypeManager::~BinaryTypeManager()
            {
                delete snapshots;
                delete pending;
            }

            SharedPointer<BinaryTypeHandler> BinaryTypeManager::GetHandler(const std::string& typeName,
                const std::string& affFieldName, int32_t typeId)
            {
                { // Locking scope.
                    CsLockGuard guard(cs);

                    std::map<int32_t, SPSnap>::iterator it = snapshots->find(typeId);
                    if (it != snapshots->end())
                        return SharedPointer<BinaryTypeHandler>(new BinaryTypeHandler(it->second));

                    for (size_t i = 0; i < pending->size(); ++i)
                    {
                        SPSnap& snap = (*pending)[i];

                        if (snap.Get()->GetTypeId() == typeId)
                            return SharedPointer<BinaryTypeHandler>(new BinaryTypeHandler(snap));
                    }
                }

                SPSnap snapshot = SPSnap(new Snap(typeName, affFieldName, typeId));

                return SharedPointer<BinaryTypeHandler>(new BinaryTypeHandler(snapshot));
            }

            void BinaryTypeManager::SubmitHandler(BinaryTypeHandler& hnd)
            {
                // If this is the very first write of a class or difference exists,
                // we need to enqueue it for write.
                if (hnd.HasUpdate())
                {
                    CsLockGuard guard(cs);

                    pending->push_back(hnd.GetUpdated());

                    ++pendingVer;
                }
            }

            int32_t BinaryTypeManager::GetVersion() const
            {
                Memory::Fence();

                return ver;
            }

            bool BinaryTypeManager::IsUpdatedSince(int32_t oldVer) const
            {
                Memory::Fence();

                return pendingVer > oldVer;
            }

            bool BinaryTypeManager::ProcessPendingUpdates(IgniteError& err)
            {
                CsLockGuard guard(cs);

                if (!updater)
                {
                    err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Updater is not set");

                    return false;
                }

                for (std::vector<SPSnap>::iterator it = pending->begin(); it != pending->end(); ++it)
                {
                    Snap* pendingSnap = it->Get();

                    if (!pendingSnap)
                        continue; // Snapshot has been processed already.

                    if (!updater->Update(*pendingSnap, err))
                    {
                        err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Can not send update");

                        return false; // Stop as we cannot move further.
                    }

                    std::map<int32_t, SPSnap>::iterator elem = snapshots->lower_bound(pendingSnap->GetTypeId());

                    if (elem == snapshots->end() || elem->first != pendingSnap->GetTypeId())
                        snapshots->insert(elem, std::make_pair(pendingSnap->GetTypeId(), *it));
                    else
                    {
                        // Temporary snapshot.
                        SPSnap tmp;

                        // Move all values from pending update.
                        tmp.Swap(*it);

                        // Add old fields. Only non-existing values added.
                        tmp.Get()->CopyFieldsFrom(elem->second.Get());

                        // Move to snapshots storage.
                        tmp.Swap(elem->second);
                    }
                }

                pending->clear();

                ver = pendingVer;

                return true;
            }

            void BinaryTypeManager::SetUpdater(BinaryTypeUpdater* updater)
            {
                CsLockGuard guard(cs);

                this->updater = updater;
            }

            SPSnap BinaryTypeManager::GetMeta(int32_t typeId)
            {
                CsLockGuard guard(cs);

                std::map<int32_t, SPSnap>::iterator it = snapshots->find(typeId);

                if (it != snapshots->end() && it->second.Get())
                    return it->second;

                for (size_t i = 0; i < pending->size(); ++i)
                {
                    SPSnap& snap = (*pending)[i];

                    if (snap.Get()->GetTypeId() == typeId)
                        return snap;
                }

                if (!updater)
                    throw IgniteError(IgniteError::IGNITE_ERR_BINARY, "Metadata updater is not available.");

                IgniteError err;

                SPSnap snap = updater->GetMeta(typeId, err);

                IgniteError::ThrowIfNeeded(err);

                // Caching meta snapshot for faster access in future.
                snapshots->insert(std::make_pair(typeId, snap));

                return snap;
            }
        }
    }
}
