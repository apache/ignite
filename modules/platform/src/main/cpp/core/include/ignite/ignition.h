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
 * \mainpage Apache Ignite C++ Library
 *
 * The Apache Ignite is a proven software solution, which delivers unprecedented speed
 * and unlimited scale to accelerate your business and time to insights. It enables high-performance transactions,
 * real-time streaming and fast analytics in a single, comprehensive data access and processing layer. The In-Memory
 * Data Fabric is designed to easily power both existing and new applications in a distributed, massively
 * parallel architecture on affordable, industry-standard hardware.
 * <p>
 * The Apache Ignite provides a unified API that spans all key types of applications
 * (Java, .NET, C++) and connects them with multiple data stores containing structured, semi-structured and
 * unstructured data (SQL, NoSQL, Hadoop). It offers a secure, highly available and manageable data environment
 * that allows companies to process full ACID transactions and generate valuable insights from real-time,
 * interactive and batch queries.
 * <p>
 * The In-Memory Data Fabric offers a strategic approach to in-memory computing that delivers performance,
 * scale and comprehensive capabilities far above and beyond what traditional in-memory databases,
 * data grids or other in-memory-based point solutions can offer by themselves.
 *
 * \section ultimate_speed_and_scale Ultimate Speed and Scale
 *
 * The Apache Ignite accesses and processes data from distributed enterprise and
 * cloud-based data stores orders of magnitudes faster, and shares them with today's most demanding transactional,
 * analytical and hybrid applications. The In-Memory Data Fabric delivers unprecedented throughput
 * and low latency performance in a virtually unlimited, global scale-out architecture for both new and
 * existing applications.
 *
 * \section comprehensive_and_proven Comprehensive and Proven
 *
 * The Apache Ignite is the product of seven years of meticulous research and development,
 * built from the ground up (i.e. no bolt-on's), and run successfully by hundreds of organizations worldwide.
 * It is a comprehensive in-memory solution that includes a clustering and compute grid, a database-agnostic data grid,
 * a real-time streaming engine as well as plug-and-play Hadoop acceleration. The In-Memory Data Fabric
 * connects multiple data sources (relational, NoSQL, Hadoop) with Java, .NET and C++ applications, and offers
 * a secure and highly available architecture; it also provides a fully-featured, graphical management interface.
 * <p>
 * The Apache Ignite is used today by Fortune 500 companies, top government agencies as well as
 * innovative mobile and web companies in a broad range of business scenarios, such as automated trading systems,
 * fraud detection, big data analytics, social networks, online gaming, and bioinformatics.
 */

#ifndef _IGNITE_IGNITION
#define _IGNITE_IGNITION

#include "ignite/ignite.h"
#include "ignite/ignite_configuration.h"
#include "ignite/ignite_error.h"

namespace ignite
{
    /**
     * This class defines a factory for the main Ignite API.
     */
    class IGNITE_IMPORT_EXPORT Ignition
    {
    public:
        /**
         * Start Ignite instance.
         *
         * @param cfg Configuration.
         * @return Ignite instance or null in case of error.
         */
        static Ignite Start(const IgniteConfiguration& cfg);

        /*
         * Start Ignite instance.
         *
         * @param cfg Configuration.
         * @param err Error.
         * @return Ignite instance or null in case of error.
         */
        static Ignite Start(const IgniteConfiguration& cfg, IgniteError* err);

        /**
         * Start Ignite instance with specific name.
         *
         * @param cfg Configuration.
         * @param name Ignite name.
         * @return Ignite instance or null in case of error.
         */
        static Ignite Start(const IgniteConfiguration& cfg, const char* name);

        /**
         * Start Ignite instance with specific name.
         *
         * @param cfg Configuration.
         * @param name Ignite name.
         * @param err Error.
         * @return Ignite instance or null in case of error.
         */
        static Ignite Start(const IgniteConfiguration& cfg, const char* name, IgniteError* err);

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
        static Ignite Get(IgniteError* err);

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
        static Ignite Get(const char* name, IgniteError* err);

        /**
         * Stop default Ignite instance.
         *
         * @param cancel Cancel flag.
         * @return True if default Ignite instance was stopped by this call.
         */
        static bool Stop(const bool cancel);

        /**
         * Stop default Ignite instance.
         *
         * @param cancel Cancel flag.
         * @param err Error.
         * @return True if Ignite instance was stopped by this call.
         */
        static bool Stop(const bool cancel, IgniteError* err);

        /**
         * Stop Ignite instance with the given name.
         *
         * @param name Ignite name.
         * @param cancel Cancel flag.
         * @return True if Ignite instance was stopped by this call.
         */
        static bool Stop(const char* name, const bool cancel);

        /**
         * Stop Ignite instance with the given name.
         *
         * @param name Ignite name.
         * @param cancel Cancel flag.
         * @param err Error.
         * @return True if Ignite instance was stopped by this call.
         */
        static bool Stop(const char* name, const bool cancel, IgniteError* err);

        /**
         * Stop all running Ignite instances.
         *
         * @param cancel Cancel flag.
         */
        static void StopAll(const bool cancel);

        /**
         * Stop all running Ignite instances.
         *
         * @param cancel Cancel flag.
         * @param err Error.
         */
        static void StopAll(const bool cancel, IgniteError* err);
    };    
}

#endif