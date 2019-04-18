GridGain OSGi Karaf Integration Module
-------------------------------------------

This module contains a feature repository to facilitate installing GridGain into an Apache Karaf container.

Use the following Karaf command:

    karaf@root()> feature:repo-add mvn:org.gridgain/ignite-osgi-karaf/${ignite.version}/xml/features

Replacing ${ignite.version} with the GridGain version you would like to install.

You may now list the GridGain features that are available for installation:

    karaf@root()> feature:list | grep ignite

Each feature installs the corresponding ignite module + its dependencies.

We include an global feature with name 'ignite-all' that collectively installs all GridGain features at once.
