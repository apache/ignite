/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace GridGain.Examples.Portable
{
    /// <summary>
    /// Employee.
    /// </summary>
    [Serializable]
    public class Employee
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <param name="salary">Salary.</param>
        /// <param name="address">Address.</param>
        /// <param name="departments">Departments.</param>
        public Employee(string name, long salary, Address address, ICollection<string> departments)
        {
            Name = name;
            Salary = salary;
            Address = address;
            Departments = departments;
        }

        /// <summary>
        /// Name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Salary.
        /// </summary>
        public long Salary { get; set; }

        /// <summary>
        /// Address.
        /// </summary>
        public Address Address { get; set; }

        /// <summary>
        /// Departments.
        /// </summary>
        public ICollection<string> Departments { get; set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        override public string ToString()
        {
            return string.Format("{0} [name={1}, salary={2}, address={3}, departments={4}]", typeof(Employee).Name, 
                Name, Salary, Address, CollectionToString(Departments));
        }

        /// <summary>
        /// Get string representation of collection.
        /// </summary>
        /// <returns></returns>
        private static string CollectionToString<T>(ICollection<T> col)
        {
            if (col == null)
                return "null";

            var elements = col.Any() 
                ? col.Select(x => x.ToString()).Aggregate((x, y) => x + ", " + y) 
                : string.Empty;

            return string.Format("[{0}]", elements);
        }
    }
}
