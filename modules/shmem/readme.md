<center>
![GridGain Logo](http://www.gridgain.com/images/logo/logo_mid.png "GridGain Logo")
</center>

<div style="height: 5px"></div>

## Building the Shmem Library

Built the C++ shmem library and copy it into `GRIDGAIN_HOME/modules/core/src/main/java/META-INF/native/<os.name>`.
To build the shmem library you need to execute in `GRIDGAIN_HOME/shmem` folder the following commands:

    ./configure --libdir=GRIDGAIN_HOME/modules/core/src/main/java/META-INF/native/<os.name>
    make install
    
