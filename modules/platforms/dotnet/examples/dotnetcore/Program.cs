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

namespace Apache.Ignite.Examples
{
    using System;

    /// <summary>
    /// Examples selector - program entry point.
    /// </summary>
    public static class Program
    {
        /// <summary>
        /// Runs the program.
        /// </summary>
        [STAThread]
        public static void Main(string[] args)
        {
            if (args.Length == 1 && args[0] == "--all")
            {
                Write("Running all examples unattended ...");

                PutGetExample.Run();
                SqlExample.Run();
                LinqExample.Run();

                return;
            }

            while (true)
            {
                Write("======================================");
                Write("Welcome to Apache Ignite.NET Examples!");
                Write("Choose an example to run:");
                Write("1. Cache put-get");
                Write("2. SQL");
                Write("3. LINQ");
                Write("4. Exit examples");

                switch (ReadNumber())
                {
                    case 1:
                        Write("Starting cache put-get example ...");
                        PutGetExample.Run();
                        break;
                    case 2:
                        Write("Starting SQL example ...");
                        SqlExample.Run();
                        break;
                    case 3:
                        Write("Starting LINQ example ...");
                        LinqExample.Run();
                        break;
                    case 4:
                        return;
                }

                Write();
                Write("Example finished, press any key to continue ...");
                Console.ReadKey();
            }
        }

        /// <summary>
        /// Reads the number from console.
        /// </summary>
        private static int ReadNumber()
        {
            Write("Enter a number: ");

            while (true)
            {
                var input = Console.ReadLine();

                if (!int.TryParse(input, out var id))
                {
                    Write("Not a number, try again: ");
                }
                else if (id < 1 || 4 < id)
                {
                    Write("Out of range, try again: ");
                }
                else
                {
                    return id;
                }
            }
        }

        /// <summary>
        /// Writes string to console.
        /// </summary>
        private static void Write(string s = null) => Console.WriteLine(s == null ? null : $">>> {s}");
    }
}