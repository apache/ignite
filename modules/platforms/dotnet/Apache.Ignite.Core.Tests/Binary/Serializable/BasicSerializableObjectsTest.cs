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

namespace Apache.Ignite.Core.Tests.Binary.Serializable
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.ComponentModel;
    using System.Configuration;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using Microsoft.CSharp.RuntimeBinder;
    using Newtonsoft.Json.Serialization;
    using NUnit.Framework;

    /// <summary>
    /// Tests basic ISerializable scenarios.
    /// </summary>
    public class BasicSerializableObjectsTest
    {
#pragma warning disable CS0618
        /// <summary>
        /// Source:
        /// https://docs.microsoft.com/en-us/dotnet/standard/serialization/binary-serialization#serializable-types
        /// </summary>
        private static readonly IReadOnlyList<object> SerializableTypeObjects = new[]
        {
            new RuntimeBinderException(),
            new RuntimeBinderInternalCompilerException(),
            new AccessViolationException(),
            new AggregateException(),
            new AppDomainUnloadedException(),
            new ApplicationException(),
            new ArgumentException(),
            new ArgumentNullException(),
            new ArgumentOutOfRangeException(),
            new ArithmeticException(),
            new ArraySegment<object>(),
            new ArrayTypeMismatchException(),
            new SerializableAttribute(),
            new BadImageFormatException(),
            new Boolean(),
            new Byte(),
            new CannotUnloadAppDomainException(),
            new Char(),
            new ArrayList(),
            new BitArray(10),
            new Comparer(CultureInfo.InvariantCulture),
            new DictionaryEntry(),
            StringComparer.CurrentCultureIgnoreCase,
            new HashSet<int>(),
            new KeyNotFoundException(),
            new KeyValuePair<int, int>(),
            new LinkedList<int>(),
            new List<float>(),
            new Queue<long>(),
            new Dictionary<int, string>(),
            new SortedDictionary<string, string>(),
            new SortedList<float, double>(),
            new SortedSet<int>(),
            new Stack<int>(),
            new Hashtable(),
            new Collection<int>(),
            new JsonPropertyCollection(typeof(object)),
            new ObservableCollection<string>(),
            new ReadOnlyCollection<int>(Enumerable.Range(1, 10).ToList()),
            new ReadOnlyDictionary<int, string>(Enumerable.Range(1, 5).ToDictionary(x => x, x => x.ToString())),
            new ReadOnlyObservableCollection<string>(new ObservableCollection<string>()),
            new Queue(),
            new SortedList(),
            new System.Collections.Specialized.HybridDictionary(),
            new System.Collections.Specialized.ListDictionary(),
            new System.Collections.Specialized.OrderedDictionary(),
            new System.Collections.Specialized.StringCollection(),
            new System.Collections.Specialized.StringDictionary(),
            new Stack(),
            new BindingList<int>(),
#if NETCOREAPP
            new System.ComponentModel.DataAnnotations.ValidationException(),
#endif
            new System.ComponentModel.Design.CheckoutException(),
            new InvalidAsynchronousStateException(),
            new InvalidEnumArgumentException(),
            new LicenseException(typeof(object)),
            new WarningException(),
            new Win32Exception(),
            new ConfigurationErrorsException(),
            new ConfigurationException("x"),
            new System.Configuration.Provider.ProviderException(),
            new SettingsPropertyIsReadOnlyException(),
            new SettingsPropertyNotFoundException(),
            new SettingsPropertyWrongTypeException(),
            new ContextMarshalException(),
            DBNull.Value,
            new System.Data.ConstraintException(),
            new System.Data.DBConcurrencyException(),
            new System.Data.DataException(),
            new System.Data.DataSet(),
            new System.Data.DataTable(),
            new System.Data.DeletedRowInaccessibleException(),
            new System.Data.DuplicateNameException(),
            new System.Data.EvaluateException(),
            new System.Data.InRowChangingEventException(),
            new System.Data.InvalidConstraintException(),
            new System.Data.InvalidExpressionException(),
            new System.Data.MissingPrimaryKeyException(),
            new System.Data.NoNullAllowedException(),
            new System.Data.PropertyCollection(),
            new System.Data.ReadOnlyException(),
            new System.Data.RowNotInTableException(),
            new System.Data.SqlTypes.SqlAlreadyFilledException(),
            new System.Data.SqlTypes.SqlBoolean(),
            new System.Data.SqlTypes.SqlByte(),
            new System.Data.SqlTypes.SqlDateTime(),
            new System.Data.SqlTypes.SqlDouble(),
            new System.Data.SqlTypes.SqlGuid(),
            new System.Data.SqlTypes.SqlInt16(),
            new System.Data.SqlTypes.SqlInt32(),
            new System.Data.SqlTypes.SqlInt64(),
            new System.Data.SqlTypes.SqlNotFilledException(),
            new System.Data.SqlTypes.SqlNullValueException(),
            new System.Data.SqlTypes.SqlString(),
            new System.Data.SqlTypes.SqlTruncateException(),
            new System.Data.SqlTypes.SqlTypeException(),
            new System.Data.StrongTypingException(),
            new System.Data.SyntaxErrorException(),
            new System.Data.VersionNotFoundException(),
            new DataMisalignedException(),
            new DateTime(),
            new DateTimeOffset(),
            new Decimal(),
            new System.Diagnostics.Tracing.EventSourceException(),
            new DirectoryNotFoundException(),
            new DivideByZeroException(),
            new DllNotFoundException(),
            new Double(),
            new System.Drawing.Color(),
            new System.Drawing.Point(),
            new System.Drawing.PointF(),
            new System.Drawing.Rectangle(),
            new System.Drawing.RectangleF(),
            new System.Drawing.Size(),
            new System.Drawing.SizeF(),
            new DuplicateWaitObjectException(),
            new EntryPointNotFoundException(),
            EventArgs.Empty,
            new Exception(),
            new ExecutionEngineException(),
            new FieldAccessException(),
            new FormatException(),
            new CultureNotFoundException(),
            new SortVersion(1, Guid.NewGuid()),
            new Guid(),
            new DriveNotFoundException(),
            new EndOfStreamException(),
            new FileLoadException(),
            new FileNotFoundException(),
            new IOException(),
            new InternalBufferOverflowException(),
            new InvalidDataException(),
            new System.IO.IsolatedStorage.IsolatedStorageException(),
            new PathTooLongException(),
            new IndexOutOfRangeException(),
            new InsufficientExecutionStackException(),
            new InsufficientMemoryException(),
            new Int16(),
            new Int32(),
            new Int64(),
            new IntPtr(),
            new InvalidCastException(),
            new InvalidOperationException(),
            new InvalidProgramException(),
            new InvalidTimeZoneException(),
            new MemberAccessException(),
            new MethodAccessException(),
            new MissingFieldException(),
            new MissingMemberException(),
            new MissingMethodException(),
            new MulticastNotSupportedException(),
            new System.Net.Cookie(),
            new System.Net.CookieCollection(),
            new System.Net.CookieContainer(),
            new System.Net.CookieException(),
            new System.Net.HttpListenerException(),
            new System.Net.Mail.SmtpException(),
            new System.Net.Mail.SmtpFailedRecipientException(),
            new System.Net.Mail.SmtpFailedRecipientsException(),
            new System.Net.NetworkInformation.NetworkInformationException(),
            new System.Net.NetworkInformation.PingException("x"),
            new System.Net.ProtocolViolationException(),
            new System.Net.Sockets.SocketException(),
            new System.Net.WebException(),
            new System.Net.WebSockets.WebSocketException(),
#if NETCOREAPP
            new NotFiniteNumberException(),
#endif
            new NotImplementedException(),
            new NotSupportedException(),
            new NullReferenceException(),
            new System.Numerics.BigInteger(),
            new System.Numerics.Complex(),
            new Object(),
            new ObjectDisposedException("x"),
            new OperationCanceledException(),
            new OutOfMemoryException(),
            new OverflowException(),
            new PlatformNotSupportedException(),
            new RankException(),
            new System.Reflection.AmbiguousMatchException(),
            new System.Reflection.CustomAttributeFormatException(),
            new System.Reflection.InvalidFilterCriteriaException(),
            new System.Reflection.ReflectionTypeLoadException(null, null),
            new System.Reflection.TargetException(),
            new System.Reflection.TargetInvocationException("x", null),
            new System.Reflection.TargetParameterCountException(),
            new System.Resources.MissingManifestResourceException(),
            new System.Resources.MissingSatelliteAssemblyException(),
#if NETCOREAPP
            new System.Runtime.CompilerServices.RuntimeWrappedException(new object()),
#endif
            new System.Runtime.InteropServices.COMException(),
            new System.Runtime.InteropServices.ExternalException(),
            new System.Runtime.InteropServices.InvalidComObjectException(),
            new System.Runtime.InteropServices.InvalidOleVariantTypeException(),
            new System.Runtime.InteropServices.MarshalDirectiveException(),
            new System.Runtime.InteropServices.SEHException(),
            new System.Runtime.InteropServices.SafeArrayRankMismatchException(),
            new System.Runtime.InteropServices.SafeArrayTypeMismatchException(),
            new InvalidDataContractException(),
            new SerializationException(),
            new SByte(),

#if !NETCOREAPP
            new System.Security.AccessControl.PrivilegeNotHeldException(),
            new System.Security.Authentication.AuthenticationException(),
            new System.Security.Authentication.InvalidCredentialException(),
            new System.Security.Cryptography.CryptographicException(),
            new System.Security.Cryptography.CryptographicUnexpectedOperationException(),
            new System.Security.HostProtectionException(),
            new System.Security.Policy.PolicyException(),
            new System.Security.Principal.IdentityNotMappedException(),
            new System.Security.SecurityException(),
            new System.Security.VerificationException(),
            new System.Security.XmlSyntaxException(),
#endif

            new Single(),
            new StackOverflowException(),
            new SystemException(),
            new System.Text.DecoderFallbackException(),
            new System.Text.EncoderFallbackException(),
            new System.Text.RegularExpressions.RegexMatchTimeoutException(),
            new System.Text.StringBuilder(),
            new System.Threading.AbandonedMutexException(),
            new System.Threading.BarrierPostPhaseException(),
            new System.Threading.LockRecursionException(),
            new System.Threading.SemaphoreFullException(),
            new System.Threading.SynchronizationLockException(),
            new System.Threading.Tasks.TaskCanceledException(),
            new System.Threading.Tasks.TaskSchedulerException(),
            new System.Threading.ThreadInterruptedException(),
            new System.Threading.ThreadStateException(),
            new System.Threading.WaitHandleCannotBeOpenedException(),
            new TimeSpan(),
            TimeZoneInfo.Utc,
            new TimeZoneNotFoundException(),
            new TimeoutException(),
            new System.Transactions.TransactionAbortedException(),
            new System.Transactions.TransactionException(),
            new System.Transactions.TransactionInDoubtException(),
            new System.Transactions.TransactionManagerCommunicationException(),
            new System.Transactions.TransactionPromotionException(),
            new TypeAccessException(),
            new TypeInitializationException("x", new Exception()),
            new TypeLoadException(),
            new TypeUnloadedException(),
            new UInt16(),
            new UInt32(),
            new UInt64(),
            new UIntPtr(),
            new UnauthorizedAccessException(),
            new Uri("https://example.com"),
            new UriFormatException(),
            new ValueTuple(),
            new Version(),
            new WeakReference<object>(new object()),
            new WeakReference(new object()),
            new System.Xml.Schema.XmlSchemaException(),
            new System.Xml.Schema.XmlSchemaInferenceException(),
            new System.Xml.Schema.XmlSchemaValidationException(),
            new System.Xml.XPath.XPathException(),
            new System.Xml.XmlException(),
            new System.Xml.Xsl.XsltCompileException(),
            new System.Xml.Xsl.XsltException()
        };
#pragma warning restore CS0618

        /// <summary>
        /// Tests the object with no fields.
        /// </summary>
        [Test]
        public void TestEmptyObject()
        {
            var res = TestUtils.SerializeDeserialize(new EmptyObject());

            Assert.IsNotNull(res);
        }

        /// <summary>
        /// Tests the object with no fields.
        /// </summary>
        [Test]
        public void TestEmptyObjectOnline()
        {
            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            using (var ignite2 = Ignition.Start(TestUtils.GetTestConfiguration(name: "1")))
            {
                var cache = ignite.CreateCache<int, EmptyObject>("c");

                cache[1] = new EmptyObject();

                var res = ignite2.GetCache<int, EmptyObject>("c")[1];

                Assert.IsNotNull(res);
            }
        }

        /// <summary>
        /// Tests ISerializable without serialization ctor.
        /// </summary>
        [Test]
        public void TestMissingCtor()
        {
            var ex = Assert.Throws<SerializationException>(() => TestUtils.SerializeDeserialize(new MissingCtor()));
            Assert.AreEqual(string.Format("The constructor to deserialize an object of type '{0}' was not found.",
                typeof(MissingCtor)), ex.Message);
        }

        /// <summary>
        /// Tests <see cref="Type"/> serialization.
        /// </summary>
        [Test]
        public void TestTypes()
        {
            var type = GetType();

            var res = TestUtils.SerializeDeserialize(type);

            Assert.AreEqual(type.AssemblyQualifiedName, res.AssemblyQualifiedName);
        }

        /// <summary>
        /// Tests a special case with StringComparer.
        /// </summary>
        [Test]
        public void TestComparer()
        {
            var obj = StringComparer.OrdinalIgnoreCase;
            var res = TestUtils.SerializeDeserialize(obj);

            Assert.AreEqual("OrdinalComparer", res.GetType().Name);
            Assert.AreEqual(0, res.Compare("A", "a"), "Ignore case flag is deserialized incorrectly.");
        }

        /// <summary>
        /// Tests all serializable system types.
        /// </summary>
        [Test, TestCaseSource(nameof(SerializableTypeObjects))]
        public void TestAllSerializableSystemTypes(object obj)
        {
            Assert.IsNotNull(obj);
            var res = TestUtils.SerializeDeserialize(obj);

            Assert.IsNotNull(res);
        }

        /// <summary>
        /// Missing serialization ctor.
        /// </summary>
        private class MissingCtor : ISerializable
        {
            /** <inheritdoc /> */
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                // No-op.
            }
        }

        /// <summary>
        /// Object with no fields.
        /// </summary>
        [Serializable]
        private class EmptyObject : ISerializable
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="EmptyObject"/> class.
            /// </summary>
            public EmptyObject()
            {
                // No-op.
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="EmptyObject"/> class.
            /// </summary>
            private EmptyObject(SerializationInfo info, StreamingContext context)
            {
                Assert.AreEqual(StreamingContextStates.All, context.State);
                Assert.IsNull(context.Context);
            }

            /** <inheritdoc /> */
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                Assert.AreEqual(StreamingContextStates.All, context.State);
                Assert.IsNull(context.Context);
            }
        }
    }
}
