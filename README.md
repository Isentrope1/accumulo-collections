
Accumulo Collections v0.2 README

Accumulo Collections is a lightweight library by [Isentropy](http://isentropy.com) that dramatically simplifies development of fast NoSQL applications by encapsulating many powerful, distributed features of [Accumulo](https://accumulo.apache.org) in the familiar Java Collections interface. Accumulo is a giant sorted map with rich server-side functionality, and our [AccumuloSortedMap](https://github.com/isentropy/accumulo-collections/blob/master/src/main/java/com/isentropy/accumulo/collections/AccumuloSortedMap.java) is a robust java [SortedMap](https://docs.oracle.com/javase/7/docs/api/java/util/SortedMap.html) implementation that is backed by an Accumulo table. It handles serialization and foreign keys, and provides extensive server-side features like entry timeout, aggregates, filtering, efficient one-to-many mapping, partitioning and sampling. You can add your own Accumulo [iterators](https://accumulo.apache.org/1.7/accumulo_user_manual.html#_iterators) to [derive new maps](https://github.com/isentropy/accumulo-collections/wiki#derived-maps-abstract-accumulo-iterators).

When used with a properly configured Accumulo and backing [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html), the collections are distributed, fault tolerant, concurrently accessible, fast and enormously scalable. 

Documentation is [here](https://github.com/isentropy/accumulo-collections/wiki).

Published under Apache Software License 2.0, see [LICENSE](https://github.com/isentropy/accumulo-collections/blob/master/LICENSE).
