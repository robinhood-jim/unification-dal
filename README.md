#
Unification Data Access Layer 
aim to support all kinds of FileSystem(including Apache VFS2 support,HDFS etc),all Kind of Db Like resource(relation Database,NoSql support sql like operation),and provide unified access api to eliminated the complexity and duplicated  coding among them;


I.Feature

    1.support Commons VFS file System and hdfs file System;
    2.support All Relation Database System;
    3.support NoSql repository(Hbase/Cassandra/mongodb and etc);
    4.support Message Queue repository(ActiveMQ/rabbitMQ/kafka);
    5.using unificated api to access Resource through any of the two above repository;
    6.can process with Compress File,now Support (gz/zip/bzip2/lzo/snappy/LZMA/);
 
II.code sample
  

	
   
         
