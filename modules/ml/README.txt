Apache Ignite Math Module
------------------------------

Apache Ignite Math module provides several implementations of vector and matrices.

Module includes on heap and off heap, dense and sparse, local and distributed implementations.

Based on ideas from Apache Mahout.

# Local build with javadoc

Run from project root:
mvn clean package -Pml -DskipTests -pl modules/ml -am

Find generated jars in target folder.