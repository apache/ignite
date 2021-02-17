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

// ReSharper disable once CheckNamespace
namespace Apache.Ignite.Platform.Model
{
    using System;

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
            // ReSharper disable once NonReadonlyMemberInGetHashCode
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

    /// <summary>
    /// A class is a clone of Java class Account with the same namespace.
    /// </summary>
    public class Account
    {
        public String Id { get; set; }

        public int Amount { get; set; }

        protected bool Equals(Account other)
        {
            return Id == other.Id;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Account) obj);
        }

        public override int GetHashCode()
        {
            // ReSharper disable once NonReadonlyMemberInGetHashCode
            return Id.GetHashCode();
        }
    }

    /// <summary>
    /// A enum is a clone of Java class User with the same namespace.
    /// </summary>
    // ReSharper disable InconsistentNaming
    public enum ACL
    {
        ALLOW, DENY
    }
    
    /// <summary>
    /// A enum is a clone of Java class AccessLevel with the same namespace.
    /// </summary>
    public enum AccessLevel
    {
        USER, SUPER
    }

    // ReSharper enable InconsistentNaming
    /// <summary>
    /// A class is a clone of Java class Role with the same namespace.
    /// </summary>
    public class Role
    {
        public String Name { get; set; }

        /** Tests declaration as System.Enum. */
        public Enum AccessLevel { get; set; }
    }

    /// <summary>
    /// A class is a clone of Java class User with the same namespace.
    /// </summary>
    public class User
    {
        public int Id { get; set; }

        public ACL Acl { get; set; }

        public Role Role { get; set; }
    }

    /// <summary>
    /// A class is a clone of Java class ParamValue with the same namespace.
    /// </summary>
    public class ParamValue
    {
        /** */
        public int Id { get; set; }

        /** */
        public long Val { get; set; }
    }

    /// <summary>
    /// A class is a clone of Java class Parameter with the same namespace.
    /// </summary>
    public class Parameter
    {
        /** */
        public int Id { get; set; }

        /** */
        public ParamValue[] Values { get; set; }
    }

    /// <summary>
    /// A class is a clone of Java class V1 with the same namespace.
    /// </summary>
    public class V1 { public String Name { get; set; } }

    /// <summary>
    /// A class is a clone of Java class V2 with the same namespace.
    /// </summary>
    public class V2 { public String Name { get; set; } }

    /// <summary>
    /// A class is a clone of Java class V3 with the same namespace.
    /// </summary>
    public class V3 { public String Name { get; set; } }

    /// <summary>
    /// A class is a clone of Java class V4 with the same namespace.
    /// </summary>
    public class V4 { public String Name { get; set; } }

    /// <summary>
    /// A class is a clone of Java class V5 with the same namespace.
    /// </summary>
    public class V5 { public String Name { get; set; } }

    /// <summary>
    /// A class is a clone of Java class V6 with the same namespace.
    /// </summary>
    public class V6 { public String Name { get; set; } }

    /// <summary>
    /// A class is a clone of Java class V7 with the same namespace.
    /// </summary>
    public class V7 { public String Name { get; set; } }

    /// <summary>
    /// A class is a clone of Java class V8 with the same namespace.
    /// </summary>
    public class V8 { public String Name { get; set; } }

    /// <summary>
    /// A class is a clone of Java class V9 with the same namespace.
    /// </summary>
    public class V9 { public String Name { get; set; } }

    /// <summary>
    /// A class is a clone of Java class V10 with the same namespace.
    /// </summary>
    public class V10 { public String Name { get; set; } }

    /// <summary>
    /// A class is a clone of Java class V11 with the same namespace.
    /// </summary>
    public class V11 { public String Name { get; set; } }

    /// <summary>
    /// A class is a clone of Java class V12 with the same namespace.
    /// </summary>
    public class V12 { public String Name { get; set; } }
}
