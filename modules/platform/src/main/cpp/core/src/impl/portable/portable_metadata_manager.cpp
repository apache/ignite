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

#include <ignite/common/concurrent.h>

#include "ignite/impl/portable/portable_metadata_manager.h"

using namespace ignite::common::concurrent;

namespace ignite
{    
    namespace impl
    {
        namespace portable
        {
            PortableMetadataManager::PortableMetadataManager() : 
                snapshots(SharedPointer<std::map<int32_t, SPSnap>>(new std::map<int32_t, SPSnap>)),
                pending(new std::vector<SPSnap>()), 
                cs(new CriticalSection()), 
                pendingVer(0), ver(0)
            {
                // No-op.
            }

            PortableMetadataManager::~PortableMetadataManager()
            {
                pending->erase(pending->begin(), pending->end());

                delete pending;
                delete cs;
            }

            SharedPointer<PortableMetadataHandler> PortableMetadataManager::GetHandler(int32_t typeId)
            {
                SharedPointer<std::map<int32_t, SPSnap>> snapshots0 = snapshots;

                SPSnap snapshot = (*snapshots0.Get())[typeId];

                return SharedPointer<PortableMetadataHandler>(new PortableMetadataHandler(snapshot));
            }

            void PortableMetadataManager::SubmitHandler(std::string typeName, int32_t typeId, 
                PortableMetadataHandler* hnd)
            {
                Snap* snap = hnd->GetSnapshot().Get();

                // If this is the very first write of a class or difference exists, 
                // we need to enqueue it for write.
                if (!snap || hnd->HasDifference())
                {
                    std::set<int32_t>* newFieldIds = new std::set<int32_t>();
                    std::map<std::string, int32_t>* newFields = new std::map<std::string, int32_t>();
                    
                    CopyFields(snap, newFieldIds, newFields);

                    if (hnd->HasDifference())
                    {
                        std::set<int32_t>* diffFieldIds = hnd->GetFieldIds();
                        std::map<std::string, int32_t>* diffFields = hnd->GetFields();

                        for (std::set<int32_t>::iterator it = diffFieldIds->begin(); it != diffFieldIds->end(); ++it)
                            newFieldIds->insert(*it);

                        for (std::map<std::string, int32_t>::iterator it = diffFields->begin(); it != diffFields->end(); ++it)
                            (*newFields)[it->first] = it->second;
                    }

                    Snap* diffSnap = new Snap(typeName, typeId, newFieldIds, newFields);

                    cs->Enter();

                    pending->push_back(SPSnap(diffSnap));

                    pendingVer++;

                    cs->Leave();
                }
            }

            int32_t PortableMetadataManager::GetVersion()
            {
                Memory::Fence();

                return ver;
            }

            bool PortableMetadataManager::IsUpdatedSince(int32_t oldVer)
            {
                Memory::Fence();

                return pendingVer > oldVer;
            }

            bool PortableMetadataManager::ProcessPendingUpdates(PortableMetadataUpdater* updater, IgniteError* err)
            {
                bool success = true; // Optimistically assume that all will be fine.
                
                cs->Enter();

                for (std::vector<SPSnap>::iterator it = pending->begin(); it != pending->end(); ++it)
                {
                    Snap* pendingSnap = (*it).Get();

                    if (updater->Update(pendingSnap, err))
                    {
                        // Perform copy-on-write update of snapshot collection.
                        std::map<int32_t, SPSnap>* newSnapshots = new std::map<int32_t, SPSnap>();
                        
                        bool snapshotFound = false;

                        for (std::map<int32_t, SPSnap>::iterator snapIt = snapshots.Get()->begin();
                            snapIt != snapshots.Get()->end(); ++snapIt)
                        {
                            int32_t curTypeId = snapIt->first;
                            Snap* curSnap = snapIt->second.Get();

                            if (pendingSnap->GetTypeId() == curTypeId)
                            {
                                // Have to create snapshot with updated fields.
                                std::set<int32_t>* newFieldIds = new std::set<int32_t>();
                                std::map<std::string, int32_t>* newFields = new std::map<std::string, int32_t>();

                                // Add old fields.
                                CopyFields(curSnap, newFieldIds, newFields);

                                // Add new fields.
                                CopyFields(pendingSnap, newFieldIds, newFields);
                                
                                // Create new snapshot.
                                Snap* newSnap = new Snap(pendingSnap->GetTypeName(), pendingSnap->GetTypeId(), 
                                    newFieldIds, newFields);

                                (*newSnapshots)[curTypeId] = SPSnap(newSnap);

                                snapshotFound = true;
                            }
                            else 
                                (*newSnapshots)[curTypeId] = snapIt->second; // Just transfer exising snapshot.
                        }

                        // Handle situation when completely new snapshot is found.
                        if (!snapshotFound)
                            (*newSnapshots)[pendingSnap->GetTypeId()] = *it;

                        snapshots = SharedPointer<std::map<int32_t, SPSnap>>(newSnapshots);
                    }
                    else
                    {
                        // Stop as we cannot move further.
                        success = false;

                        break;
                    }
                }

                if (success) 
                {
                    pending->erase(pending->begin(), pending->end());

                    ver = pendingVer;
                }

                cs->Leave();

                return success;
            }

            void PortableMetadataManager::CopyFields(Snap* snap, std::set<int32_t>* fieldIds, 
                std::map<std::string, int32_t>* fields)
            {
                if (snap && snap->HasFields())
                {
                    std::set<int32_t>* snapFieldIds = snap->GetFieldIds();
                    std::map<std::string, int32_t>* snapFields = snap->GetFields();

                    for (std::set<int32_t>::iterator oldIt = snapFieldIds->begin();
                        oldIt != snapFieldIds->end(); ++oldIt)
                        fieldIds->insert(*oldIt);

                    for (std::map<std::string, int32_t>::iterator newFieldsIt = snapFields->begin();
                        newFieldsIt != snapFields->end(); ++newFieldsIt)
                        (*fields)[newFieldsIt->first] = newFieldsIt->second;
                }
            }
        }
    }
}