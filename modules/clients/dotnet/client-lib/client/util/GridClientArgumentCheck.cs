// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Util {
    using System;

    /**
     * <summary>
     * This class encapsulates argument check (null and range) for public facing APIs. Unlike asserts
     * it throws "normal" exceptions with standardized messages.</summary>
     */
    internal static class GridClientArgumentCheck {
        /**
         * <summary>
         * Checks if given argument value is not <c>null</c>.
         * Otherwise - throws <see ctype="ArgumentException"/>.</summary>
         *
         * <param name="val">Argument value to check.</param>
         * <param name="name">Name of the argument in the code (used in error message).</param>
         */
        public static void NotNull(Object val, String name) {
            if (val == null)
                throw new ArgumentException("OMG! Argument cannot be null: " + name);
        }

        /**
         * <summary>
         * Checks that none of the given values are <c>null</c>.
         * Otherwise - throws <see ctype="ArgumentException"/>.</summary>
         *
         * <param name="val1">1st argument value to check.</param>
         * <param name="name1">Name of the 1st argument in the code (used in error message).</param>
         * <param name="val2">2nd argument value to check.</param>
         * <param name="name2">Name of the 2nd argument in the code (used in error message).</param>
         */
        public static void NotNull(Object val1, String name1, Object val2, String name2) {
            NotNull(val1, name1);
            NotNull(val2, name2);
        }

        /**
         * <summary>
         * Checks that none of the given values are <c>null</c>.
         * Otherwise - throws <see ctype="ArgumentException"/>.</summary>
         *
         * <param name="val1">1st argument value to check.</param>
         * <param name="name1">Name of the 1st argument in the code (used in error message).</param>
         * <param name="val2">2nd argument value to check.</param>
         * <param name="name2">Name of the 2nd argument in the code (used in error message).</param>
         * <param name="val3">3rd argument value to check.</param>
         * <param name="name3">Name of the 3rd argument in the code (used in error message).</param>
         */
        public static void NotNull(Object val1, String name1, Object val2, String name2, Object val3, String name3) {
            NotNull(val1, name1);
            NotNull(val2, name2);
            NotNull(val3, name3);
        }

        /**
         * <summary>
         * Checks that none of the given values are <c>null</c>.
         * Otherwise - throws <see ctype="ArgumentException"/>.</summary>
         *
         * <param name="val1">1st argument value to check.</param>
         * <param name="name1">Name of the 1st argument in the code (used in error message).</param>
         * <param name="val2">2nd argument value to check.</param>
         * <param name="name2">Name of the 2nd argument in the code (used in error message).</param>
         * <param name="val3">3rd argument value to check.</param>
         * <param name="name3">Name of the 3rd argument in the code (used in error message).</param>
         * <param name="val4">4th argument value to check.</param>
         * <param name="name4">Name of the 4th argument in the code (used in error message).</param>
         */
        public static void NotNull(Object val1, String name1, Object val2, String name2, Object val3, String name3,
            Object val4, String name4) {
            NotNull(val1, name1);
            NotNull(val2, name2);
            NotNull(val3, name3);
            NotNull(val4, name4);
        }

        /**
         * <summary>
         * Checks if given argument's condition is equal to <c>true</c>.
         * Otherwise - throws <see ctype="ArgumentException"/>.</summary>
         *
         * <param name="cond">Argument's value condition to check.</param>
         * <param name="desc">Description of the condition to be used in error message.</param>
         */
        public static void Ensure(bool cond, String desc) {
            if (!cond)
                throw new ArgumentException("OMG! Argument is invalid: " + desc);
        }
    }
}
