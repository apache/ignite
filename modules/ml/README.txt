Apache Ignite Math Module
------------------------------

Apache Ignite Math module provides several implementations of vector and matrices.

Module includes on heap and off heap, dense and sparse, local and distributed implementations.

Based on ideas from Apache Mahout.

# Local build with javadoc

Run from project root:
mvn clean package -Pml -DskipTests -pl modules/ml -am

Apache binary releases cannot include LGPL dependencies. If you would like to activate native BLAS optimizations
into your build, you should download the source release
from Ignite website and do the build with the following maven command:

mvn clean package -Pml,lgpl -DskipTests -pl modules/ml -am

Find generated jars in target folder.