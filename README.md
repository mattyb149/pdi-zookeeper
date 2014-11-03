pdi-zookeeper
=============

Description
--------------

A collection of plugins for Pentaho Data Integration (PDI) related to Zookeeper, to include:

- Zookeeper Input step, for reading configuration data stored in Zookeeper
- Zookeeper Output step, for storing configuration data in Zookeeper
- Spoon plugin for managing your Zookeeper data (uses the zooviewer project at https://code.google.com/p/zooviewer/)
 
Here's the long-term wishlist (if you'd like to contribute):
- Extension point plugin for dynamically loading cluster schemas from Zookeeper (will help manage Carte clusters)
- Zookeeper-backed PDI Repository

Compiling
-------------

Use the following to build the plugin package:

gradle clean plugin

Configuration
--------------

To configure the ZooKeeper plugin(s), you must set the property "zk" to your ZooKeeper quorum. There are a couple of ways to do this, one is to edit the config.properties file in the plugin, another is to set the property at runtime, such as:

<your java command> -Dzk=my.zookeeper-node.com:2181

Zooviewer

The plugin includes Zooviewer, available in the Tools drop-down menu under Manage ZooKeeper.
