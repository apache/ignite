<center>
![GridGain Logo](http://www.gridgain.com/images/logo/logo_mid.png "GridGain Logo")
</center>

<div style="height: 5px"></div>

## Building the Shmem Library

Built the C++ shmem library and copy it in `GRIDGAIN_HOME/modules/core/src/main/java/META-INF/native/<os.name>`, you can build the shmem by going into the `GRIDGAIN_HOME/shmem` directory and executing the following commands:

    ./configure --libdir=GRIDGAIN_HOME/modules/core/src/main/java/META-INF/native/<os.name>
    make install
    
