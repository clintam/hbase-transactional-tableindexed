This project is a transactional and indexing extension for hbase. 

Installation:
 Drop the jar in the classpath of your application
 
Configuration: 
 One of the two regionserver extensions must be turned on by setting the appropriate configuration
 (in hbase-site.xml). 
 
 To enable just the indexing extension: set 
 'hbase.regionserver.class' to 'org.apache.hadoop.hbase.ipc.IndexedRegionInterface' 
 and 
 'hbase.regionserver.impl' to 'org.apache.hadoop.hbase.regionserver.tableindexed.IndexedRegionServer'
 
 To enable the transactional extension (which includes the indexing): set 
 'hbase.regionserver.class' to 'org.apache.hadoop.hbase.ipc.TransactionalRegionInterface' 
  and
 'hbase.regionserver.impl' to 'org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegionServer'
 
 
 For more details, looks at the package.html in the appropriate client package of the source. 