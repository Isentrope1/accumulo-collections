
Accumulo Collections v0.1 README

Accumulo Collections is a lightweight library by [Isentropy](http://isentropy.com) that dramatically simplifies [Accumulo](https://accumulo.apache.org) development by enabling developers to use the powerful distributed features of Accumulo via the familiar Java Collections interface. Accumulo is a giant sorted map with rich server-side functionality, and our [AccumuloSortedMap](https://github.com/isentropy/accumulo-collections/blob/master/src/main/java/com/isentropy/accumulo/collections/AccumuloSortedMap.java) is a robust java [SortedMap](https://docs.oracle.com/javase/7/docs/api/java/util/SortedMap.html) implementation that provides many additional server-side features like entry timeout, aggregates, filtering, value transformation, partitioning and sampling. Accumulo [iterators](https://accumulo.apache.org/1.7/accumulo_user_manual.html#_iterators) can be stacked seamlessly over existing maps to make new, [derived maps](https://github.com/isentropy/accumulo-collections/wiki#derived-maps).

When used with a properly configured Accumulo and backing [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html), the collections are distributed, fault tolerant, concurrently accessible, fast and enormously scalable. 

Documentation is [here](https://github.com/isentropy/accumulo-collections/wiki).

Published under Apache Software License 2.0, see [LICENSE](https://github.com/isentropy/accumulo-collections/blob/master/LICENSE).
