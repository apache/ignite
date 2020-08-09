namespace Apache.Ignite.Core.Tests.Services
{
    using System;
    using System.Collections;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Java service proxy interface.
    /// </summary>
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public interface IJavaService
    {
        /** */
        bool isCancelled();

        /** */
        bool isInitialized();

        /** */
        bool isExecuted();

        /** */
        byte test(byte x);

        /** */
        short test(short x);

        /** */
        int test(int x);

        /** */
        long test(long x);

        /** */
        float test(float x);

        /** */
        double test(double x);

        /** */
        char test(char x);

        /** */
        string test(string x);

        /** */
        bool test(bool x);

        /** */
        DateTime test(DateTime x);

        /** */
        Guid test(Guid x);

        /** */
        byte? testWrapper(byte? x);

        /** */
        short? testWrapper(short? x);

        /** */
        int? testWrapper(int? x);

        /** */
        long? testWrapper(long? x);

        /** */
        float? testWrapper(float? x);

        /** */
        double? testWrapper(double? x);

        /** */
        char? testWrapper(char? x);

        /** */
        bool? testWrapper(bool? x);

        /** */
        byte[] testArray(byte[] x);

        /** */
        short[] testArray(short[] x);

        /** */
        int[] testArray(int[] x);

        /** */
        long[] testArray(long[] x);

        /** */
        float[] testArray(float[] x);

        /** */
        double[] testArray(double[] x);

        /** */
        char[] testArray(char[] x);

        /** */
        string[] testArray(string[] x);

        /** */
        bool[] testArray(bool[] x);

        /** */
        DateTime?[] testArray(DateTime?[] x);

        /** */
        Guid?[] testArray(Guid?[] x);

        /** */
        int test(int x, string y);

        /** */
        int test(string x, int y);

        /** */
        int? testNull(int? x);

        /** */
        DateTime? testNullTimestamp(DateTime? x);

        /** */
        Guid? testNullUUID(Guid? x);

        /** */
        int testParams(params object[] args);

        /** */
        ServicesTest.PlatformComputeBinarizable testBinarizable(ServicesTest.PlatformComputeBinarizable x);

        /** */
        object[] testBinarizableArrayOfObjects(object[] x);

        /** */
        IBinaryObject[] testBinaryObjectArray(IBinaryObject[] x);

        /** */
        ServicesTest.PlatformComputeBinarizable[] testBinarizableArray(ServicesTest.PlatformComputeBinarizable[] x);

        /** */
        ICollection testBinarizableCollection(ICollection x);

        /** */
        IBinaryObject testBinaryObject(IBinaryObject x);
    }
}