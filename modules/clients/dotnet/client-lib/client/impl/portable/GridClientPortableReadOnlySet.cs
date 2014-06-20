/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Portable
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /**
     * <summary>Read-only wrapper over set.</summary> 
     */ 
    class GridClientPortableReadOnlySet<T> : ISet<T> 
    {
        /** Underlying set. */
        private readonly ISet<T> set;

        /**
         * <summary>Constructor.</summary>
         * <param name="set">Underlying set.</param>
         */ 
        public GridClientPortableReadOnlySet(ISet<T> set)
        {
            if (set == null)
                throw new NullReferenceException("Child set cannot be null.");

            this.set = set;
        }

        /** <inheritdoc /> */
        public bool Add(T item)
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public void ExceptWith(IEnumerable<T> other)
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public void IntersectWith(IEnumerable<T> other)
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public bool IsProperSubsetOf(IEnumerable<T> other)
        {
            return set.IsProperSubsetOf(other);        
        }

        /** <inheritdoc /> */
        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            return set.IsProperSupersetOf(other);
        }

        /** <inheritdoc /> */
        public bool IsSubsetOf(IEnumerable<T> other)
        {
            return set.IsSubsetOf(other);
        }

        /** <inheritdoc /> */
        public bool IsSupersetOf(IEnumerable<T> other)
        {
            return set.IsSupersetOf(other);
        }

        /** <inheritdoc /> */
        public bool Overlaps(IEnumerable<T> other)
        {
            return set.Overlaps(other);
        }

        /** <inheritdoc /> */
        public bool SetEquals(IEnumerable<T> other)
        {
            return set.SetEquals(other);
        }

        /** <inheritdoc /> */
        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public void UnionWith(IEnumerable<T> other)
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        void ICollection<T>.Add(T item)
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public void Clear()
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public bool Contains(T item)
        {
            return set.Contains(item);
        }

        /** <inheritdoc /> */
        public void CopyTo(T[] array, int arrayIndex)
        {
            set.CopyTo(array, arrayIndex);
        }

        /** <inheritdoc /> */
        public int Count
        {
            get { return set.Count; }
        }

        /** <inheritdoc /> */
        public bool IsReadOnly
        {
            get { return true; }
        }

        /** <inheritdoc /> */
        public bool Remove(T item)
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public IEnumerator<T> GetEnumerator()
        {
            return set.GetEnumerator();
        }

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return set.GetEnumerator();
        }
    }
}
