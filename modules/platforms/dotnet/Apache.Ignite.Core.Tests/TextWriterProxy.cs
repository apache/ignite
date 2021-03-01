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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Text writer proxy with StringBuilder logging for console redirection tests.
    /// </summary>
    public class TextWriterProxy : TextWriter
    {
        /** */
        private readonly TextWriter _inner;

        /** */
        private readonly StringBuilder _out = new StringBuilder();

        /** */
        private readonly Action _disposeAction;

        public TextWriterProxy(TextWriter inner, Action disposeAction = null)
        {
            Debug.Assert(inner != null);

            _inner = inner;
            _disposeAction = disposeAction;
        }

        public static TextWriterProxy RedirectConsoleOut()
        {
            var oldOut = Console.Out;

            var proxy = new TextWriterProxy(oldOut, () => Console.SetOut(oldOut));
            Console.SetOut(proxy);

            return proxy;
        }

        public string GetText() => _out.ToString();

        public override Encoding Encoding => _inner.Encoding;

        public override void Close()
        {
            _inner.Close();
        }

        public override void Flush()
        {
            _inner.Flush();
        }

        public override Task FlushAsync()
        {
            return _inner.FlushAsync();
        }

        public override void Write(char value)
        {
            _inner.Write(value);
            _out.Append(value);
        }

        public override IFormatProvider FormatProvider => _inner.FormatProvider;

        public override string NewLine
        {
            get => _inner.NewLine;
            set => _inner.NewLine = value;
        }

        protected override void Dispose(bool disposing)
        {
            _disposeAction?.Invoke();
            base.Dispose(disposing);
        }
    }
}
