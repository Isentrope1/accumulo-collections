
Accumulo Collections v0.1 README

Accumulo Collections is a library by Isentropy that contains implementations of Java Collections backed by an Accumulo instance. Accumulo is itself a performant, distributed sorted map of byte arrays, and our AccumuloSortedMap is a robust java SortedMap implementation that simplifies Human-Accumulo Interaction by allowing developers to leverage the power and distributed features of Accumulo using the simple, familiar Java Collections interface. [AccumuloSortedMap](https://github.com/isentropy/accumulo-collections/blob/master/src/main/java/com/isentropy/accumulo/collections/AccumuloSortedMap.java) also provides a number of extra features that are not normally seen in java Collections like entry timeout, server-side aggregates, partitioning and sampling. When used with a properly configured Accumulo and backing HDFS, the collections are distributed, fault tolerant, concurrently accessible, fast and enormously scalable.

Documentation is here: https://github.com/isentropy/accumulo-collections/wiki

Email bug reports and feature requests to jwolff@isentropy.com.

