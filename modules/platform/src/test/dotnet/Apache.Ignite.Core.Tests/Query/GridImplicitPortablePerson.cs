﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Tests.Query
{
    using System;

    /**
     * 
     */
    class GridImplicitPortablePerson {
        /**
         * 
         */
        public GridImplicitPortablePerson(String _name, int _age) {
            this.name = _name;
            this.age = _age;
        }

        /**
         * 
         */
        public String name
        {
            get;
            set;
        }

        /**
         * 
         */
        public int age
        {
            get;
            set;
        }
    }
}
