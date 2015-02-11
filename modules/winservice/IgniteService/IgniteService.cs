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

namespace Ignite
{
    using System;
    using System.ServiceProcess;
    using System.Diagnostics;
    using System.Threading;
    using System.IO;

    /** <summary>Service implementation.</summary> */
    class IgniteService : ServiceBase
    {
        /** <summary>Service name.</summary> */
        private String name;

        /** <summary>External process.</summary> */
        private Process proc;

        /**
         * <summary>Creates new service.</summary>
         */
        public IgniteService()
        {
            this.ServiceName = "Ignite Service";
        }

        /**
         * <summary>Runs service.</summary>
         */
        public static void Main()
        {
            System.ServiceProcess.ServiceBase.Run(new IgniteService());
        }

        /** <inheritdoc /> */
        protected override void OnStart(string[] args)
        {
            base.OnStart(args);

            try
            {
                if (args.Length != 3)
                    throw new Exception("Following arguments must be provided: service name, script path, log path.");

                name = args[0];

                String script = args[1];

                int spaceIdx = script.IndexOf(' ');

                String scriptPath = spaceIdx != -1 ? script.Substring(0, spaceIdx) : script;
                String scriptArgs = spaceIdx != -1 ? script.Substring(spaceIdx + 1) : String.Empty;

                String log = args[2];

                proc = new Process();

                proc.StartInfo.FileName = scriptPath;
                proc.StartInfo.Arguments = scriptArgs + " > " + log + " 2>&1";
                proc.StartInfo.CreateNoWindow = true;

                proc.Start();

                Thread waitThread = new Thread(new ThreadStart(WaitStopAndDelete));

                waitThread.Start();
            }
            catch (Exception e)
            {
                EventLog.WriteEntry(e.Message);

                try
                {
                    Kill();
                }
                finally
                {
                    StopAndDelete();
                }
            }
        }

        /** <inheritdoc /> */
        protected override void OnStop()
        {
            base.OnStop();

            Kill();
        }

        /**
         * <summary>Waits for external process to complete, stops and deletes service.</summary>
         */
        private void WaitStopAndDelete()
        {
            try
            {
                proc.WaitForExit();
            }
            finally
            {
                StopAndDelete();
            }
        }

        /**
         * <summary>Stops and deletes service.</summary>
         */
        private void StopAndDelete()
        {
            try
            {
                Stop();
            }
            finally
            {
                Delete();
            }
        }

        /**
         * <summary>Kills the process.</summary>
         */
        private void Kill()
        {
            Execute("taskkill /f /t /pid " + proc.Id);
        }

        /**
         * <summary>Deletes service.</summary>
         */
        private void Delete()
        {
            Execute("sc delete " + name);
        }

        /**
         * <summary>Executes command.</summary>
         * <param name="cmd">Command.</name>
         */
        private void Execute(String cmd)
        {
            Process cmdProc = new Process();

            cmdProc.StartInfo.FileName = "cmd.exe";
            cmdProc.StartInfo.Arguments = "/c " + cmd;
            cmdProc.StartInfo.CreateNoWindow = true;

            cmdProc.Start();

            cmdProc.WaitForExit();
        }
    }
}
