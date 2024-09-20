# NUMA aware memory allocator for Apache Ignite
Allocates memory on linux using `libnuma` under the hood. 

## Requirements
`libnuma 2.0.x` must be installed on your Linux machine. It is recommended to install `numactl` utility also.
1. `RHEL` or `Cent OS`
```bash
$ sudo yum install numactl
```
2. `Ubuntu` or `Debian`
```bash
$ sudo apt install numactl
```

## Building from source
`JDK`, `C++ 11` compatible compiler (`gcc >= 4.8.5`) and `libnuma` headers
1. `RHEL` or `Cent OS`
```bash
$ sudo yum groupinstall 'Development Tools'
$ sudo yum install java-11-openjdk numactl-devel libstdc++-static
```
2. `Ubuntu` or `Debian`
```bash
$ sudo apt install build-essential libnuma-dev openjdk-11-jdk
```
## Usage
### Simple allocation strategy
1. Allocation with default NUMA policy for thread/process, uses `void *numa_alloc(size_t)` under the hood:
```xml
<property name="dataStorageConfiguration">
    <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
        <property name="defaultDataRegionConfiguration">
            <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
                <property name="name" value="Default_Region"/>
                ....
                <property name="memoryAllocator">
                    <bean class="org.apache.ignite.mem.NumaAllocator">
                        <constructor-arg>
                            <bean class="org.apache.ignite.mem.SimpleNumaAllocationStrategy"/>
                        </constructor-arg>
                    </bean>
                </property>
            </bean>
        </property>
    </bean>
</property>
```
2. Allocation on specific NUMA node, uses `void *numa_alloc_onnode(size_t, int)` under the hood:
```xml
<property name="dataStorageConfiguration">
    <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
        <property name="defaultDataRegionConfiguration">
            <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
                <property name="name" value="Default_Region"/>
                ....
                <property name="memoryAllocator">
                    <bean class="org.apache.ignite.mem.NumaAllocator">
                        <constructor-arg>
                            <bean class="org.apache.ignite.mem.SimpleNumaAllocationStrategy">
                                <constructor-arg name="node" value="0"/>
                            </bean>
                        </constructor-arg>
                    </bean>
                </property>
            </bean>
        </property>
    </bean>
</property>
```
### Interleaved allocation strategy.
1. Interleaved allocation on all NUMA nodes, uses `void *numa_alloc_interleaved(size_t)` under the hood:
```xml
<property name="dataStorageConfiguration">
    <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
        <property name="defaultDataRegionConfiguration">
            <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
                <property name="name" value="Default_Region"/>
                ....
                <property name="memoryAllocator">
                    <bean class="org.apache.ignite.mem.NumaAllocator">
                        <constructor-arg>
                            <bean class="org.apache.ignite.mem.InterleavedNumaAllocationStrategy"/>
                        </constructor-arg>
                    </bean>
                </property>
            </bean>
        </property>
    </bean>
</property>
```
2. Interleaved allocation on specified NUMA nodes, uses `void *numa_alloc_interleaved_subset(size_t, struct bitmask*)`
under the hood:
```xml
<property name="dataStorageConfiguration">
    <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
        <property name="defaultDataRegionConfiguration">
            <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
                <property name="name" value="Default_Region"/>
                ....
                <property name="memoryAllocator">
                    <bean class="org.apache.ignite.mem.NumaAllocator">
                        <constructor-arg>
                            <bean class="org.apache.ignite.mem.InterleavedNumaAllocationStrategy">
                                <constructor-arg name="nodes">
                                    <array>
                                        <value>0</value>
                                        <value>1</value>
                                    </array>
                                </constructor-arg>
                            </bean>
                        </constructor-arg>
                    </bean>
                </property>
            </bean>
        </property>
    </bean>
</property>
```
## Local node allocation strategy
Allocation on local for process NUMA node, uses `void* numa_alloc_onnode(size_t)` under the hood.
```xml
<property name="dataStorageConfiguration">
    <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
        <property name="defaultDataRegionConfiguration">
            <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
                <property name="name" value="Default_Region"/>
                ....
                <property name="memoryAllocator">
                    <bean class="org.apache.ignite.mem.NumaAllocator">
                        <constructor-arg>
                            <constructor-arg>
                                <bean class="org.apache.ignite.mem.LocalNumaAllocationStrategy"/>
                            </constructor-arg>
                        </constructor-arg>
                    </bean>
                </property>
            </bean>
        </property>
    </bean>
</property>
```
