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

#ifndef _IGNITE_ODBC_BENCHMARK_BENCHMARK_UTILS
#define _IGNITE_ODBC_BENCHMARK_BENCHMARK_UTILS

#include <boost/chrono/chrono.hpp>

#include <ignite/ignite_error.h>
#include <ignite/ignition.h>

#define IGNITE_RUN_BENCHMARK(x,r,w) ignite::RunBenchmark<x,boost::chrono::milliseconds>(#x, r, w)
#define IGNITE_RUN_BENCHMARK_D(x,r,w,d) ignite::RunBenchmark<x,d>(#x, r, w)

namespace ignite
{
    template<typename T> const char* UnitTitle() { return ""; }

    template<> inline const char* UnitTitle<boost::chrono::nanoseconds>() { return "nanoseconds"; };
    template<> inline const char* UnitTitle<boost::chrono::microseconds>() { return "microseconds"; };
    template<> inline const char* UnitTitle<boost::chrono::milliseconds>() { return "milliseconds"; };
    template<> inline const char* UnitTitle<boost::chrono::seconds>() { return "seconds"; };
    template<> inline const char* UnitTitle<boost::chrono::minutes>() { return "minutes"; };
    template<> inline const char* UnitTitle<boost::chrono::hours>() { return "hours"; };

    template<typename T, typename D>
    D Benchmark(T& test, unsigned repeats)
    {
        using namespace boost::chrono;

        D res(0);

        for (unsigned i = 0; i < repeats; ++i)
        {
            test.Prepare();

            time_point<steady_clock> begin = steady_clock::now();

            test.Run();

            time_point<steady_clock> end = steady_clock::now();

            test.Cleanup();

            res += duration_cast<D>(end - begin);
        }

        return res / repeats;
    }

    template<typename T, typename D>
    void RunBenchmark(const std::string& name, unsigned repeats, unsigned warmupRepeats = 5)
    {
        std::cout << std::endl;
        std::cout << ">>> Running benchmark " << name << "..." << std::endl;
        std::cout << std::endl;

        try
        {
            std::cout << ">>> Setting up..." << std::endl;
            std::cout << std::endl;

            T test;
            std::cout << ">>> Ready." << std::endl;
            std::cout << std::endl;

            std::cout << ">>> Warming up..." << std::endl;
            std::cout << ">>> Warm up repeats count: " << warmupRepeats << '.' << std::endl;
            std::cout << std::endl;

            Benchmark<T, D>(test, warmupRepeats);

            std::cout << ">>> Warm up finished." << std::endl;
            std::cout << std::endl;

            std::cout << ">>> Benchmarking..." << std::endl;
            std::cout << ">>> Repeats count: " << repeats << '.' << std::endl;
            std::cout << std::endl;

            D res = Benchmark<T, D>(test, repeats);

            std::cout << ">>> Finished. Execution took " << res.count() << " " << UnitTitle<D>() << '.' << std::endl;
            std::cout << std::endl;
        }
        catch(const IgniteError& e)
        {
            std::cerr << "Error: " << e.GetText() << std::endl;
        }
        catch (const std::exception& e)
        {
            std::cerr << "System error: " << e.what() << std::endl;
        }
        catch (...)
        {
            std::cerr << "Unknown error." << std::endl;
        }

        std::cout << std::endl;
    }
};

#endif // _IGNITE_ODBC_BENCHMARK_BENCHMARK_UTILS
