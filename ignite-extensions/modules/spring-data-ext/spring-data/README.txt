Apache Ignite Spring Module
---------------------------

Apache Ignite Spring Data extension provides an integration with Spring Data framework.

Main features:

- Supports multiple Ignite instances on same JVM (@RepositoryConfig).
- Supports query tuning parameters in @Query annotation
- Supports projections
- Supports Page and Stream responses
- Supports Sql Fields Query resultset transformation into the domain entity
- Supports named parameters (:myParam) into SQL queries, declared using @Param("myParam")
- Supports advanced parameter binding and SpEL expressions into SQL queries:
- Template variables:
    - #entityName - the simple class name of the domain entity
- Method parameter expressions: Parameters are exposed for indexed access ([0] is the first query method's param) or via the name declared using @Param. The actual SpEL expression binding is triggered by ?#. Example: ?#{[0] or ?#{#myParamName}
- Advanced SpEL expressions: While advanced parameter binding is a very useful feature, the real power of SpEL stems from the fact, that the expressions can refer to framework abstractions or other application components through SpEL EvaluationContext extension model.
- Supports SpEL expressions into Text queries (TextQuery).

Importing Spring Data extension In Maven Project
----------------------------------------

If you are using Maven to manage dependencies of your project, you can add Spring Data extension
dependency like this (replace '${ignite-spring-data-ext.version}' with actual version of Ignite Spring Data extension
you are interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-spring-data-ext</artifactId>
            <version>${ignite-spring-data-ext.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
