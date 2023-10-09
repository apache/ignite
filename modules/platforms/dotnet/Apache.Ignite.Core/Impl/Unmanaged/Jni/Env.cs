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

namespace Apache.Ignite.Core.Impl.Unmanaged.Jni
{
    using System;
    using System.Diagnostics;
    using System.Runtime.InteropServices;
    using System.Security;
    using System.Text;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// JNIEnv.
    /// </summary>
    [SuppressUnmanagedCodeSecurity]
    internal sealed unsafe class Env
    {
        /** JNIEnv pointer. */
        private readonly IntPtr _envPtr;

        /** JVM. */
        private readonly Jvm _jvm;

        /** */
        private readonly EnvDelegates.CallStaticVoidMethod _callStaticVoidMethod;

        /** */
        private readonly EnvDelegates.CallStaticBooleanMethod _callStaticBoolMethod;

        /** */
        private readonly EnvDelegates.CallStaticIntMethod _callStaticIntMethod;

        /** */
        private readonly EnvDelegates.FindClass _findClass;

        /** */
        private readonly EnvDelegates.GetMethodId _getMethodId;

        /** */
        private readonly EnvDelegates.GetStaticMethodId _getStaticMethodId;

        /** */
        private readonly EnvDelegates.NewString _newString;

        /** */
        private readonly EnvDelegates.ExceptionOccurred _exceptionOccurred;

        /** */
        private readonly EnvDelegates.GetObjectClass _getObjectClass;

        /** */
        private readonly EnvDelegates.CallObjectMethod _callObjectMethod;

        /** */
        private readonly EnvDelegates.CallLongMethod _callLongMethod;

        /** */
        private readonly EnvDelegates.CallVoidMethod _callVoidMethod;

        /** */
        private readonly EnvDelegates.GetStringCritical _getStringCritical;

        /** */
        private readonly EnvDelegates.GetStringLength _getStringLength;

        /** */
        private readonly EnvDelegates.ReleaseStringCritical _releaseStringCritical;

        /** */
        private readonly EnvDelegates.ExceptionClear _exceptionClear;

        /** */
        private readonly EnvDelegates.ExceptionCheck _exceptionCheck;

        /** */
        private readonly EnvDelegates.CallStaticObjectMethod _callStaticObjectMethod;

        /** */
        private readonly EnvDelegates.RegisterNatives _registerNatives;

        /** */
        private readonly EnvDelegates.DeleteLocalRef _deleteLocalRef;

        /** */
        private readonly EnvDelegates.NewGlobalRef _newGlobalRef;

        /** */
        private readonly EnvDelegates.DeleteGlobalRef _deleteGlobalRef;

        /** */
        private readonly EnvDelegates.ThrowNew _throwNew;

        /// <summary>
        /// Initializes a new instance of the <see cref="Env" /> class.
        /// </summary>
        internal Env(IntPtr envPtr, Jvm jvm)
        {
            Debug.Assert(envPtr != IntPtr.Zero);
            Debug.Assert(jvm != null);

            _envPtr = envPtr;
            _jvm = jvm;

            var funcPtr = (EnvInterface**)envPtr;
            var func = **funcPtr;

            GetDelegate(func.CallStaticVoidMethod, out _callStaticVoidMethod);
            GetDelegate(func.CallStaticBooleanMethod, out _callStaticBoolMethod);
            GetDelegate(func.CallStaticIntMethod, out _callStaticIntMethod);
            GetDelegate(func.FindClass, out _findClass);
            GetDelegate(func.GetMethodID, out _getMethodId);
            GetDelegate(func.GetStaticMethodID, out _getStaticMethodId);
            GetDelegate(func.NewString, out _newString);
            GetDelegate(func.ExceptionOccurred, out _exceptionOccurred);
            GetDelegate(func.ExceptionClear, out _exceptionClear);
            GetDelegate(func.ExceptionCheck, out _exceptionCheck);
            GetDelegate(func.GetObjectClass, out _getObjectClass);
            GetDelegate(func.CallObjectMethod, out _callObjectMethod);
            GetDelegate(func.CallStaticObjectMethod, out _callStaticObjectMethod);
            GetDelegate(func.CallLongMethod, out _callLongMethod);
            GetDelegate(func.CallVoidMethod, out _callVoidMethod);

            GetDelegate(func.GetStringLength, out _getStringLength);
            GetDelegate(func.GetStringCritical, out _getStringCritical);
            GetDelegate(func.ReleaseStringCritical, out _releaseStringCritical);

            GetDelegate(func.RegisterNatives, out _registerNatives);
            GetDelegate(func.DeleteLocalRef, out _deleteLocalRef);
            GetDelegate(func.NewGlobalRef, out _newGlobalRef);
            GetDelegate(func.DeleteGlobalRef, out _deleteGlobalRef);
            GetDelegate(func.ThrowNew, out _throwNew);
        }

        /// <summary>
        /// Gets the JVM.
        /// </summary>
        public Jvm Jvm
        {
            get { return _jvm; }
        }

        /// <summary>
        /// Gets the env ptr.
        /// </summary>
        public IntPtr EnvPtr
        {
            get { return _envPtr; }
        }

        /// <summary>
        /// Calls the static void method.
        /// </summary>
        public void CallStaticVoidMethod(GlobalRef cls, IntPtr methodId, long* argsPtr = null)
        {
            _callStaticVoidMethod(_envPtr, cls.Target, methodId, argsPtr);

            ExceptionCheck();
        }

        /// <summary>
        /// Calls the static bool method.
        /// </summary>
        public bool CallStaticBoolMethod(GlobalRef cls, IntPtr methodId, long* argsPtr = null)
        {
            var res = _callStaticBoolMethod(_envPtr, cls.Target, methodId, argsPtr);

            ExceptionCheck();

            return res > 0;
        }

        /// <summary>
        /// Calls a static int method.
        /// </summary>
        public int CallStaticIntMethod(GlobalRef cls, IntPtr methodId, long* argsPtr = null)
        {
            var res = _callStaticIntMethod(_envPtr, cls.Target, methodId, argsPtr);

            ExceptionCheck();

            return res;
        }

        /// <summary>
        /// Calls the object method.
        /// </summary>
        public GlobalRef CallObjectMethod(GlobalRef obj, IntPtr methodId, long* argsPtr = null)
        {
            var lref = _callObjectMethod(_envPtr, obj.Target, methodId, argsPtr);

            ExceptionCheck();

            return NewGlobalRef(lref);
        }

        /// <summary>
        /// Calls the long method.
        /// </summary>
        public long CallLongMethod(GlobalRef obj, IntPtr methodId, long* argsPtr = null)
        {
            var res = _callLongMethod(_envPtr, obj.Target, methodId, argsPtr);

            ExceptionCheck();

            return res;
        }

        /// <summary>
        /// Calls the void method.
        /// </summary>
        public void CallVoidMethod(GlobalRef obj, IntPtr methodId, long* argsPtr = null)
        {
            _callVoidMethod(_envPtr, obj.Target, methodId, argsPtr);

            ExceptionCheck();
        }

        /// <summary>
        /// Calls the static object method.
        /// </summary>
        public GlobalRef CallStaticObjectMethod(GlobalRef cls, IntPtr methodId, long* argsPtr = null)
        {
            var res = _callStaticObjectMethod(_envPtr, cls.Target, methodId, argsPtr);

            ExceptionCheck();

            return NewGlobalRef(res);
        }

        /// <summary>
        /// Finds the class.
        /// </summary>
        public GlobalRef FindClass(string name)
        {
            var res = _findClass(_envPtr, name);

            if (res == IntPtr.Zero)
            {
                var msg = "Java class is not found (did you set IGNITE_HOME environment variable?): " + name;

                throw new IgniteException(msg);
            }

            return NewGlobalRef(res);
        }

        /// <summary>
        /// Gets the object class.
        /// </summary>
        private GlobalRef GetObjectClass(GlobalRef obj)
        {
            var res = _getObjectClass(_envPtr, obj.Target);

            ExceptionCheck();

            return NewGlobalRef(res);
        }

        /// <summary>
        /// Gets the static method identifier.
        /// </summary>
        public IntPtr GetStaticMethodId(GlobalRef clazz, string name, string signature)
        {
            var res = _getStaticMethodId(_envPtr, clazz.Target, name, signature);

            if (res == IntPtr.Zero)
            {
                throw new IgniteException("Java class method is not found (did you set IGNITE_HOME environment " +
                                          "variable?): " + name + " " + signature);
            }

            return res;
        }

        /// <summary>
        /// Gets the method identifier.
        /// </summary>
        public IntPtr GetMethodId(GlobalRef clazz, string name, string signature)
        {
            var res = _getMethodId(_envPtr, clazz.Target, name, signature);

            if (res == IntPtr.Zero)
            {
                throw new IgniteException("Java class method is not found (did you set IGNITE_HOME environment " +
                                          "variable?): " + name + " " + signature);
            }

            return res;
        }

        /// <summary>
        /// Creates new jstring from UTF16 chars.
        /// </summary>
        private GlobalRef NewStringUtf16(IntPtr utf16, int len)
        {
            if (utf16 == IntPtr.Zero)
            {
                return null;
            }

            var res = _newString(_envPtr, utf16, len);

            ExceptionCheck();

            return NewGlobalRef(res);
        }

        /// <summary>
        /// Creates new jstring from string.
        /// </summary>
        public GlobalRef NewString(string str)
        {
            if (str == null)
            {
                return null;
            }

            // .NET uses UTF-16 to represent strings internally - no conversion required, just take a pointer.
            fixed (char* strPtr = str)
            {
                return NewStringUtf16(new IntPtr(strPtr), str.Length);
            }
        }

        /// <summary>
        /// Gets UTF16 chars from jstring.
        /// </summary>
        private IntPtr GetStringCritical(IntPtr jstring)
        {
            Debug.Assert(jstring != IntPtr.Zero);

            byte isCopy;
            return _getStringCritical(_envPtr, jstring, &isCopy);
        }

        /// <summary>
        /// Releases the chars allocated by <see cref="GetStringCritical"/>.
        /// </summary>
        private void ReleaseStringCritical(IntPtr jstring, IntPtr chars)
        {
            _releaseStringCritical(_envPtr, jstring, chars);
        }

        /// <summary>
        /// Gets the length of the jstring.
        /// </summary>
        private int GetStringLength(IntPtr jstring)
        {
            Debug.Assert(jstring != IntPtr.Zero);

            return _getStringLength(_envPtr, jstring);
        }

        /// <summary>
        /// Registers the native callbacks.
        /// </summary>
        public void RegisterNatives(GlobalRef clazz, NativeMethod[] methods)
        {
            Debug.Assert(methods != null);

            fixed (NativeMethod* m = &methods[0])
            {
                var res = _registerNatives(_envPtr, clazz.Target, m, methods.Length);

                if (res != JniResult.Success)
                {
                    throw new IgniteException("Failed to register natives: " + res);
                }
            }
        }

        /// <summary>
        /// Converts jstring to string.
        /// </summary>
        private string JStringToString(GlobalRef jstring)
        {
            if (jstring == null)
            {
                return null;
            }

            return JStringToString(jstring.Target);
        }

        /// <summary>
        /// Converts jstring to string.
        /// </summary>
        public string JStringToString(IntPtr jstring)
        {
            if (jstring == IntPtr.Zero)
            {
                return null;
            }

            // 1. Use UTF16 to avoid Java-specific "modified UTF-8" issues.
            //    https://stackoverflow.com/questions/32205446/getting-true-utf-8-characters-in-java-jni
            // 2. Use GetStringCritical to avoid copying (when possible).
            //    NOTE: The code block between GetStringCritical and ReleaseStringCritical
            //    must not perform JNI calls (except GetStringLength) or block the thread.
            var charsPtr = GetStringCritical(jstring);
            if (charsPtr == IntPtr.Zero)
            {
                return null;
            }

            try
            {
                var charCount = GetStringLength(jstring);
                var byteCount = charCount * 2; // UTF16 => x2 bytes.

                return Encoding.Unicode.GetString((byte*)charsPtr, byteCount);
            }
            finally
            {
                ReleaseStringCritical(jstring, charsPtr);
            }
        }

        /// <summary>
        /// Creates a new global reference from a local reference pointer.
        /// </summary>
        public GlobalRef NewGlobalRef(IntPtr lref)
        {
            if (lref == IntPtr.Zero)
            {
                return null;
            }

            var res = new GlobalRef(_newGlobalRef(_envPtr, lref), _jvm);

            _deleteLocalRef(_envPtr, lref);

            return res;
        }

        /// <summary>
        /// Deletes the global reference.
        /// </summary>
        public void DeleteGlobalRef(IntPtr gref)
        {
            _deleteGlobalRef(_envPtr, gref);
        }

        /// <summary>
        /// Throws an exception to java.
        /// </summary>
        public void ThrowToJava(Exception e)
        {
            Debug.Assert(e != null);
            var msgChars = IgniteUtils.StringToUtf8Unmanaged(e.Message);

            try
            {
                using (var cls = FindClass("org/apache/ignite/IgniteException"))
                {
                    _throwNew(_envPtr, cls.Target, new IntPtr(msgChars));
                }
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(msgChars));
            }
        }

        /// <summary>
        /// Checks for the JNI exception and throws.
        /// </summary>
        private void ExceptionCheck()
        {
            var res = _exceptionCheck(_envPtr);
            if (res == 0)
            {
                return;
            }

            var err = _exceptionOccurred(_envPtr);

            if (err == IntPtr.Zero)
            {
                throw new IgniteException("Inconsistent JNI ExceptionCheck status.");
            }

            _exceptionClear(_envPtr);

            using (var errRef = NewGlobalRef(err))
            {
                var errRef0 = (long) errRef.Target;
                var methodId = _jvm.MethodId;

                using (var cls = GetObjectClass(errRef))
                using (var clsName = CallObjectMethod(cls, methodId.ClassGetName))
                using (var msg = CallObjectMethod(errRef, methodId.ThrowableGetMessage))
                using (var trace = CallStaticObjectMethod(methodId.PlatformUtils,
                    methodId.PlatformUtilsGetStackTrace, &errRef0))
                {
                    throw new JavaException(
                        JStringToString(clsName),
                        JStringToString(msg),
                        JStringToString(trace));
                }
            }
        }

        /// <summary>
        /// Gets the delegate.
        /// </summary>
        private static void GetDelegate<T>(IntPtr ptr, out T del)
        {
            del = (T) (object) Marshal.GetDelegateForFunctionPointer(ptr, typeof(T));
        }
    }
}
