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

namespace org.apache.ignite.platform
{
    /// <summary>
    /// A class is a clone of Java class Address with the same namespace.
    /// </summary>
    public class Address
    {
        /** */
        public string Zip { get; set; }

        /** */
        public string Addr { get; set; }
    }
    
    /// <summary>
    /// A class is a clone of Java class Department with the same namespace.
    /// </summary>
    public class Department
    {
        /** */
        public string Name { get; set; }
    }
    
    /// <summary>
    /// A class is a clone of Java class Employee with the same namespace.
    /// </summary>
    public class Employee
    {
        /** */
        public string Fio { get; set; }

        /** */
        public long Salary { get; set; }
    }

    /// <summary>
    /// A class is a clone of Java class Employee with the same namespace.
    /// </summary>
    public class Key
    {
        public long Id { get; set; }

        protected bool Equals(Key other)
        {
            return Id == other.Id;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Key) obj);
        }

        public override int GetHashCode()
        {
            return Id.GetHashCode();
        }
    }

    /// <summary>
    /// A class is a clone of Java class Employee with the same namespace.
    /// </summary>
    public class Value
    {
        public string Val { get; set; }
    }
}
