<center>
![GridGain Logo](http://www.gridgain.com/images/logo/logo_mid.png "GridGain Logo")
</center>

<div style="height: 20px"></div>

## GridGain In-Memory Computing Platform
<blockquote>In-Memory Computing uses high-performance, integrated, distributed memory systems to compute and transact on large-scale data sets in real-time, orders of magnitude faster than possible with traditional disk-based or flash technologies.
</blockquote>

GridGain’s In-Memory Computing Platform is designed to deliver uncompromised performance for a widest set of in-memory computing use cases from high performance computing, to the industry most advanced data grid, to streaming and plug-n-play Hadoop accelerator:

### In-Memory Data Grid
Natively distributed, ACID transactional, MVCC-based, SQL+NoSQL, in-memory object key-value store. The only in-memory data grid proven to scale to billions of transactions per second on commodity hardware.

### In-Memory Streaming
Massively distributed processing meets Complex Event Processing (CEP) and Streaming Processing with advanced workflow support, windowing, user-defined indexes and more.

### In-Memory Accelerator for Hadoop
Combination of In-Memory File System 100% compatible with Hadoop HDFS and In-Memory MapReduce delivering 100x performance increase. Minimal integration, plug-n-play acceleration with any Hadoop distro.

## Maven Install
The easiest way to get started with GridGain in your project is to use Maven dependency management:

### Platform Edition (includes everything)
`Platform` edition includes all GridGain editions: `data grid`, `streaming`, and `hadoop accelerator`. This
edition is required in order to compile and build GridGain source code.

```xml
<dependency>
    <groupId>org.gridgain</groupId>
    <artifactId>gridgain-platform</artifactId>
    <version>${gridgain.version}</version>
    <type>pom</type>
</dependency>
```

### HPC Edition
`HPC` edition includes all GridGain functionality except for `data grid`, `streaming` and `hadoop accelerator`.

```xml
<dependency>
    <groupId>org.gridgain</groupId>
    <artifactId>gridgain-hpc</artifactId>
    <version>${gridgain.version}</version>
    <type>pom</type>
</dependency>
```

### Data Grid Edition
`Data Grid` edition includes all GridGain functionality except for `streaming` and `hadoop accelerator`.

```xml
<dependency>
    <groupId>org.gridgain</groupId>
    <artifactId>gridgain-datagrid</artifactId>
    <version>${gridgain.version}</version>
    <type>pom</type>
</dependency>
```

### Streaming Edition
`Streaming` edition includes all GridGain functionality except for `data grid` and `hadoop accelerator`.

```xml
<dependency>
    <groupId>org.gridgain</groupId>
    <artifactId>gridgain-streaming</artifactId>
    <version>${gridgain.version}</version>
    <type>pom</type>
</dependency>
```

### Apache Hadoop Accelerator
`Hadoop Accelerator` edition includes all GridGain functionality except for `data grid` and `streaming`.

```xml
<dependency>
    <groupId>org.gridgain</groupId>
    <artifactId>gridgain-hadoop</artifactId>
    <version>${gridgain.version}</version>
    <type>pom</type>
</dependency>
```

You can copy and paste this snippet into your Maven POM file. Make sure to replace version with the one you need.

## Binary Downloads & Documentation
Grab the latest binary release and current documentation at [www.gridgain.org](http://www.gridgain.org)

## Issues
Use GitHub [issues](https://github.com/gridgain/gridgain/issues) to file bugs.

## License
GridGain is available under [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) license.

## Copyright
Copyright (C) 2007-2014, GridGain Systems, Inc. All Rights Reserved.
