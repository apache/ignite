Apache Ignite OSGi Karaf Integration Module
-------------------------------------------

This module contains a feature repository to facilitate installing Apache Ignite into an Apache Karaf container.

Use the following Karaf command:

    karaf@root()> feature:repo-add mvn:org.apache.ignite/ignite-osgi-karaf/${ignite.version}/xml/features

Replacing ${ignite.version} with the Apache Ignite version you woudl like to install.

You may now list the Ignite features that are available for installation:

    karaf@root()> feature:list | grep ignite

Each feature installs the corresponding ignite module + its dependencies.

We include an global feature with name 'ignite-all' that collectively installs all Ignite features at once.
