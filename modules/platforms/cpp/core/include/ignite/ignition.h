/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

/**
 * @file
 * Declares ignite::Ignition class.
 */

#ifndef _IGNITE_IGNITION
#define _IGNITE_IGNITION

#include <ignite/ignite_error.h>

#include "ignite/ignite.h"
#include "ignite/ignite_configuration.h"

namespace ignite
{
    /**
     * This class defines a factory for the main %Ignite API.
     */
    class IGNITE_IMPORT_EXPORT Ignition
    {
    public:
        /**
         * Start Ignite instance.
         *
         * @param cfg Configuration.
         * @return Ignite instance.
         */
        static Ignite Start(const IgniteConfiguration& cfg);

        /**
         * Start Ignite instance.
         *
         * @param cfg Configuration.
         * @param err Error.
         * @return Ignite instance.
         */
        static Ignite Start(const IgniteConfiguration& cfg, IgniteError& err);

        /**
         * Start Ignite instance with specific name.
         *
         * @param cfg Configuration.
         * @param name Ignite name.
         * @return Ignite instance.
         */
        static Ignite Start(const IgniteConfiguration& cfg, const char* name);

        /**
         * Start Ignite instance with specific name.
         *
         * @param cfg Configuration.
         * @param name Ignite name.
         * @param err Error.
         * @return Ignite instance.
         */
        static Ignite Start(const IgniteConfiguration& cfg, const char* name, IgniteError& err);

        /**
         * Get default Ignite instance.
         *
         * @return Default Ignite instance.
         */
        static Ignite Get();

        /**
         * Get default Ignite instance.
         *
         * @param err Error.
         * @return Default Ignite instance.
         */
        static Ignite Get(IgniteError& err);

        /**
         * Get Ignite instance with the given name.
         *
         * @param name Ignite name.
         * @return Ignite instance.
         */
        static Ignite Get(const char* name);

        /**
         * Get Ignite instance with the given name.
         *
         * @param name Ignite name.
         * @param err Error.
         * @return Ignite instance.
         */
        static Ignite Get(const char* name, IgniteError& err);

        /**
         * Stop default Ignite instance.
         *
         * @param cancel Cancel flag.
         * @return True if default Ignite instance was stopped by this call.
         */
        static bool Stop(bool cancel);

        /**
         * Stop default Ignite instance.
         *
         * @param cancel Cancel flag.
         * @param err Error.
         * @return True if Ignite instance was stopped by this call.
         */
        static bool Stop(bool cancel, IgniteError& err);

        /**
         * Stop Ignite instance with the given name.
         *
         * @param name Ignite name.
         * @param cancel Cancel flag.
         * @return True if Ignite instance was stopped by this call.
         */
        static bool Stop(const char* name, bool cancel);

        /**
         * Stop Ignite instance with the given name.
         *
         * @param name Ignite name.
         * @param cancel Cancel flag.
         * @param err Error.
         * @return True if Ignite instance was stopped by this call.
         */
        static bool Stop(const char* name, bool cancel, IgniteError& err);

        /**
         * Stop all running Ignite instances.
         *
         * @param cancel Cancel flag.
         */
        static void StopAll(bool cancel);

        /**
         * Stop all running Ignite instances.
         *
         * @param cancel Cancel flag.
         * @param err Error.
         */
        static void StopAll(bool cancel, IgniteError& err);
    };
}

#endif //_IGNITE_IGNITION