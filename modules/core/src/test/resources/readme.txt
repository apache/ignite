This folder is created for test GridTaskUriDeploymentDeadlockSelfTest. It contains helloworld.gar and helloworld1.gar
which are the same and were copied from Apache Ignite GAR example.

We put two files here to have a collision and make deployment SPI to unregister class loaders.

This is intended to test GG-2852 issue.
