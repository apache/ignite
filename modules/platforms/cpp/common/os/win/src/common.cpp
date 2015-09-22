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

#include <windows.h>

#include "ignite/common/common.h"
#include "ignite/common/concurrent.h"
#include "ignite/common/java.h"

using namespace ignite::common::concurrent;
using namespace ignite::common::java;

namespace ignite
{
    namespace common
    {
        void AttachHelper::OnThreadAttach()
        {
            // No-op.
        }
    }
}

BOOL WINAPI DllMain(_In_ HINSTANCE hinstDLL, _In_ DWORD fdwReason, _In_ LPVOID lpvReserved)
{
    switch (fdwReason)
    {
        case DLL_PROCESS_ATTACH:
            if (!ThreadLocal::OnProcessAttach())
                return FALSE;

            break;

        case DLL_THREAD_DETACH:
            ThreadLocal::OnThreadDetach();

            JniContext::Detach();

            break;

        case DLL_PROCESS_DETACH:
            ThreadLocal::OnProcessDetach();

            break;

        default:
            break;
    }

    return TRUE;
}