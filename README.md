
Accumulo Collections v0.1 README

## Overview
Accumulo Collections is a library by [Isentropy](http://isentropy.com) that contains implementations of Java Collections backed by an [Accumulo](https://accumulo.apache.org) instance. Accumulo is a giant sorted map with rich server-side functionality, and our [AccumuloSortedMap](https://github.com/isentropy/accumulo-collections/blob/master/src/main/java/com/isentropy/accumulo/collections/AccumuloSortedMap.java) is a robust java [SortedMap](https://docs.oracle.com/javase/7/docs/api/java/util/SortedMap.html) implementation that simplifies human-Accumulo interaction by allowing developers to use the powerful distributed features of Accumulo via the familiar Java Collections interface. When used with a properly configured Accumulo and backing [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html), the collections are distributed, fault tolerant, concurrently accessible, fast and enormously scalable. 

AccumuloSortedMap also provides a number of extra features that are not normally seen in java Collections like entry timeout, server-side aggregates, partitioning and sampling. Accumulo [iterators](https://accumulo.apache.org/1.7/accumulo_user_manual.html#_iterators) can be stacked seamlessly over existing maps to make new, derived maps. 


