# RDD Read & Write Support

This package supports read & write operations for edges and vertices and perfectly
fits into a DataFrame- or GraphFrame-centric analytics environment.

The current implementation of write requests creates Ignite caches for the provided
edge and vertex dataframes.

