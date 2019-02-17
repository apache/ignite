Apache Ignite ML Grid Module
----------------------------

Apache Ignite ML Grid module provides machine learning features, along with involved methods of linear algebra and relevant data structures, including on heap and off heap, dense and sparse, local and distributed implementations.

# Local build with javadoc

Run from project root:
mvn clean package -DskipTests -pl modules/ml -am

Apache binary releases cannot include LGPL dependencies. If you would like to activate native BLAS optimizations
into your build, you should download the source release
from Ignite website and do the build with the following maven command:

mvn clean package -Plgpl -DskipTests -pl modules/ml -am

Find generated jars in target folder.
