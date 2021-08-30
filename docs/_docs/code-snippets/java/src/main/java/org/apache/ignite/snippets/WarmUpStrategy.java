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
//tag::warm-all-regions[]
IgniteConfiguration cfg = new IgniteConfiguration();

DataStorageConfiguration storageCfg = new DataStorageConfiguration();

//Changing the default warm-up strategy for all data regions
storageCfg.setDefaultWarmUpConfiguration(new LoadAllWarmUpConfiguration());

cfg.setDataStorageConfiguration(storageCfg);
//end::warm-all-regions[]
//tag::warm-specific-region[]
IgniteConfiguration cfg = new IgniteConfiguration();

DataStorageConfiguration storageCfg = new DataStorageConfiguration();

//Setting another warm-up strategy for a custom data region
DataRegionConfiguration myNewDataRegion = new DataRegionConfiguration();

myNewDataRegion.setName("NewDataRegion");

//You can tweak the initial size as well as other settings
myNewDataRegion.setInitialSize(100 * 1024 * 1024);

//Performing data loading from disk in DRAM on restarts.
myNewDataRegion.setWarmUpConfiguration(new LoadAllWarmUpConfiguration());

//Enabling Ignite persistence. Ignite reads data from disk when queried for tables/caches from this region.
myNewDataRegion.setPersistenceEnabled(true);

//Applying the configuration.
storageCfg.setDataRegionConfigurations(myNewDataRegion);

cfg.setDataStorageConfiguration(storageCfg);
//end::warm-specific-region[]
//tag::disable-all-regions[]
IgniteConfiguration cfg = new IgniteConfiguration();

DataStorageConfiguration storageCfg = new DataStorageConfiguration();

storageCfg.setDefaultWarmUpConfiguration(new NoOpWarmUpConfiguration());

cfg.setDataStorageConfiguration(storageCfg);
//end::disable-all-regions[]
//tag::disable-specific-region[]
IgniteConfiguration cfg = new IgniteConfiguration();

DataStorageConfiguration storageCfg = new DataStorageConfiguration();

//Setting another warm-up strategy for a custom data region
DataRegionConfiguration myNewDataRegion = new DataRegionConfiguration();

myNewDataRegion.setName("NewDataRegion");

//You can tweak the initial size as well as other settings
myNewDataRegion.setInitialSize(100 * 1024 * 1024);

//Skip data loading from disk in DRAM on restarts.
myNewDataRegion.setWarmUpConfiguration(new NoOpWarmUpConfiguration());

//Enabling Ignite persistence. Ignite reads data from disk when queried for tables/caches from this region.
myNewDataRegion.setPersistenceEnabled(true);

//Applying the configuration.
storageCfg.setDataRegionConfigurations(myNewDataRegion);

cfg.setDataStorageConfiguration(storageCfg);
//end::disable-specific-region[]