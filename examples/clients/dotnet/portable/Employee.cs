/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Example.Portable
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using GridGain.Client.Portable;

    /// <summary>
    /// Employe class.
    /// </summary>
    [GridClientPortableId(100)]
    public class Employee
    {
        /** ID. */
        private Guid id;

        /** Name. */
        private string name;

        /** Salary. */
        private long salary;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">ID.</param>
        /// <param name="name">Name.</param>
        /// <param name="salary">Salary.</param>
        public Employee(Guid id, string name, long salary)
        {
            this.id = id;
            this.name = name;
            this.salary = salary;
        }

        /// <summary>
        /// ID.
        /// </summary>
        public Guid Id
        {
            get { return id; }
            set { id = value; }
        }

        /// <summary>
        /// Name.
        /// </summary>
        public string Name
        {
            get { return name; }
            set { name = value; }
        }

        /// <summary>
        /// Salary.
        /// </summary>
        public long Salary
        {
            get { return salary; }
            set { salary = value; }
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            if (obj != null && obj is Employee)
            {
                Employee that = (Employee)obj;

                return id.Equals(that.id) && name.Equals(that.name) && salary == that.salary;
            }
            else
                return false;
        }

        /** <inheritdoc /> */
        override public String ToString()
        {
            return new StringBuilder()
                .Append(typeof(Employee).Name)
                .Append(" [Id=").Append(Id)
                .Append(", name=").Append(name)
                .Append(", salary=").Append(salary)
                .Append(']').ToString();
        }
    }
}
