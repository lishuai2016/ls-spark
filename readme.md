# 可以直接运行ls.spark.WordCount类输出如下

本地模式的Wordcount，报错不用管，那是找Hadoop的东西

可以通过引入System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.7.3");解决

```java
D:\soft-install\jdk8\bin\java "-javaagent:D:\idea-2017-new\IntelliJ IDEA 2017.2.2\lib\idea_rt.jar=57568:D:\idea-2017-new\IntelliJ IDEA 2017.2.2\bin" -Dfile.encoding=UTF-8 -classpath D:\soft-install\jdk8\jre\lib\charsets.jar;D:\soft-install\jdk8\jre\lib\deploy.jar;D:\soft-install\jdk8\jre\lib\ext\access-bridge-64.jar;D:\soft-install\jdk8\jre\lib\ext\cldrdata.jar;D:\soft-install\jdk8\jre\lib\ext\dnsns.jar;D:\soft-install\jdk8\jre\lib\ext\jaccess.jar;D:\soft-install\jdk8\jre\lib\ext\jfxrt.jar;D:\soft-install\jdk8\jre\lib\ext\localedata.jar;D:\soft-install\jdk8\jre\lib\ext\nashorn.jar;D:\soft-install\jdk8\jre\lib\ext\sunec.jar;D:\soft-install\jdk8\jre\lib\ext\sunjce_provider.jar;D:\soft-install\jdk8\jre\lib\ext\sunmscapi.jar;D:\soft-install\jdk8\jre\lib\ext\sunpkcs11.jar;D:\soft-install\jdk8\jre\lib\ext\zipfs.jar;D:\soft-install\jdk8\jre\lib\javaws.jar;D:\soft-install\jdk8\jre\lib\jce.jar;D:\soft-install\jdk8\jre\lib\jfr.jar;D:\soft-install\jdk8\jre\lib\jfxswt.jar;D:\soft-install\jdk8\jre\lib\jsse.jar;D:\soft-install\jdk8\jre\lib\management-agent.jar;D:\soft-install\jdk8\jre\lib\plugin.jar;D:\soft-install\jdk8\jre\lib\resources.jar;D:\soft-install\jdk8\jre\lib\rt.jar;D:\IdeaProjects\ls-spark\target\classes;D:\IdeaProjects\ls-spark\lib\json-lib-2.2.3-jdk15.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-streaming_2.11\2.0.0\spark-streaming_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-core_2.11\2.0.0\spark-core_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\avro\avro-mapred\1.7.7\avro-mapred-1.7.7-hadoop2.jar;C:\Users\lishuai29\.m2\repository\org\apache\avro\avro-ipc\1.7.7\avro-ipc-1.7.7-tests.jar;C:\Users\lishuai29\.m2\repository\com\twitter\chill_2.11\0.8.0\chill_2.11-0.8.0.jar;C:\Users\lishuai29\.m2\repository\com\esotericsoftware\kryo-shaded\3.0.3\kryo-shaded-3.0.3.jar;C:\Users\lishuai29\.m2\repository\com\esotericsoftware\minlog\1.3.0\minlog-1.3.0.jar;C:\Users\lishuai29\.m2\repository\org\objenesis\objenesis\2.1\objenesis-2.1.jar;C:\Users\lishuai29\.m2\repository\com\twitter\chill-java\0.8.0\chill-java-0.8.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\xbean\xbean-asm5-shaded\4.4\xbean-asm5-shaded-4.4.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-client\2.2.0\hadoop-client-2.2.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-hdfs\2.2.0\hadoop-hdfs-2.2.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-mapreduce-client-app\2.2.0\hadoop-mapreduce-client-app-2.2.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-mapreduce-client-common\2.2.0\hadoop-mapreduce-client-common-2.2.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-yarn-client\2.2.0\hadoop-yarn-client-2.2.0.jar;C:\Users\lishuai29\.m2\repository\com\google\inject\guice\3.0\guice-3.0.jar;C:\Users\lishuai29\.m2\repository\javax\inject\javax.inject\1\javax.inject-1.jar;C:\Users\lishuai29\.m2\repository\aopalliance\aopalliance\1.0\aopalliance-1.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-yarn-server-common\2.2.0\hadoop-yarn-server-common-2.2.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-mapreduce-client-shuffle\2.2.0\hadoop-mapreduce-client-shuffle-2.2.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-yarn-api\2.2.0\hadoop-yarn-api-2.2.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-mapreduce-client-jobclient\2.2.0\hadoop-mapreduce-client-jobclient-2.2.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-launcher_2.11\2.0.0\spark-launcher_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-network-common_2.11\2.0.0\spark-network-common_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-network-shuffle_2.11\2.0.0\spark-network-shuffle_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\org\fusesource\leveldbjni\leveldbjni-all\1.8\leveldbjni-all-1.8.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-unsafe_2.11\2.0.0\spark-unsafe_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\net\java\dev\jets3t\jets3t\0.7.1\jets3t-0.7.1.jar;C:\Users\lishuai29\.m2\repository\org\apache\curator\curator-recipes\2.4.0\curator-recipes-2.4.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\curator\curator-framework\2.4.0\curator-framework-2.4.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\curator\curator-client\2.4.0\curator-client-2.4.0.jar;C:\Users\lishuai29\.m2\repository\javax\servlet\javax.servlet-api\3.1.0\javax.servlet-api-3.1.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\commons\commons-lang3\3.3.2\commons-lang3-3.3.2.jar;C:\Users\lishuai29\.m2\repository\com\google\code\findbugs\jsr305\1.3.9\jsr305-1.3.9.jar;C:\Users\lishuai29\.m2\repository\org\slf4j\jul-to-slf4j\1.7.16\jul-to-slf4j-1.7.16.jar;C:\Users\lishuai29\.m2\repository\org\slf4j\jcl-over-slf4j\1.7.16\jcl-over-slf4j-1.7.16.jar;C:\Users\lishuai29\.m2\repository\com\ning\compress-lzf\1.0.3\compress-lzf-1.0.3.jar;C:\Users\lishuai29\.m2\repository\org\roaringbitmap\RoaringBitmap\0.5.11\RoaringBitmap-0.5.11.jar;C:\Users\lishuai29\.m2\repository\commons-net\commons-net\2.2\commons-net-2.2.jar;C:\Users\lishuai29\.m2\repository\org\json4s\json4s-jackson_2.11\3.2.11\json4s-jackson_2.11-3.2.11.jar;C:\Users\lishuai29\.m2\repository\org\json4s\json4s-core_2.11\3.2.11\json4s-core_2.11-3.2.11.jar;C:\Users\lishuai29\.m2\repository\org\json4s\json4s-ast_2.11\3.2.11\json4s-ast_2.11-3.2.11.jar;C:\Users\lishuai29\.m2\repository\org\scala-lang\scalap\2.11.0\scalap-2.11.0.jar;C:\Users\lishuai29\.m2\repository\org\scala-lang\scala-compiler\2.11.0\scala-compiler-2.11.0.jar;C:\Users\lishuai29\.m2\repository\org\glassfish\jersey\core\jersey-client\2.22.2\jersey-client-2.22.2.jar;C:\Users\lishuai29\.m2\repository\javax\ws\rs\javax.ws.rs-api\2.0.1\javax.ws.rs-api-2.0.1.jar;C:\Users\lishuai29\.m2\repository\org\glassfish\hk2\hk2-api\2.4.0-b34\hk2-api-2.4.0-b34.jar;C:\Users\lishuai29\.m2\repository\org\glassfish\hk2\hk2-utils\2.4.0-b34\hk2-utils-2.4.0-b34.jar;C:\Users\lishuai29\.m2\repository\org\glassfish\hk2\external\aopalliance-repackaged\2.4.0-b34\aopalliance-repackaged-2.4.0-b34.jar;C:\Users\lishuai29\.m2\repository\org\glassfish\hk2\external\javax.inject\2.4.0-b34\javax.inject-2.4.0-b34.jar;C:\Users\lishuai29\.m2\repository\org\glassfish\hk2\hk2-locator\2.4.0-b34\hk2-locator-2.4.0-b34.jar;C:\Users\lishuai29\.m2\repository\org\javassist\javassist\3.18.1-GA\javassist-3.18.1-GA.jar;C:\Users\lishuai29\.m2\repository\org\glassfish\jersey\core\jersey-common\2.22.2\jersey-common-2.22.2.jar;C:\Users\lishuai29\.m2\repository\javax\annotation\javax.annotation-api\1.2\javax.annotation-api-1.2.jar;C:\Users\lishuai29\.m2\repository\org\glassfish\jersey\bundles\repackaged\jersey-guava\2.22.2\jersey-guava-2.22.2.jar;C:\Users\lishuai29\.m2\repository\org\glassfish\hk2\osgi-resource-locator\1.0.1\osgi-resource-locator-1.0.1.jar;C:\Users\lishuai29\.m2\repository\org\glassfish\jersey\core\jersey-server\2.22.2\jersey-server-2.22.2.jar;C:\Users\lishuai29\.m2\repository\org\glassfish\jersey\media\jersey-media-jaxb\2.22.2\jersey-media-jaxb-2.22.2.jar;C:\Users\lishuai29\.m2\repository\javax\validation\validation-api\1.1.0.Final\validation-api-1.1.0.Final.jar;C:\Users\lishuai29\.m2\repository\org\glassfish\jersey\containers\jersey-container-servlet\2.22.2\jersey-container-servlet-2.22.2.jar;C:\Users\lishuai29\.m2\repository\org\glassfish\jersey\containers\jersey-container-servlet-core\2.22.2\jersey-container-servlet-core-2.22.2.jar;C:\Users\lishuai29\.m2\repository\org\apache\mesos\mesos\0.21.1\mesos-0.21.1-shaded-protobuf.jar;C:\Users\lishuai29\.m2\repository\io\netty\netty\3.8.0.Final\netty-3.8.0.Final.jar;C:\Users\lishuai29\.m2\repository\com\clearspring\analytics\stream\2.7.0\stream-2.7.0.jar;C:\Users\lishuai29\.m2\repository\io\dropwizard\metrics\metrics-core\3.1.2\metrics-core-3.1.2.jar;C:\Users\lishuai29\.m2\repository\io\dropwizard\metrics\metrics-jvm\3.1.2\metrics-jvm-3.1.2.jar;C:\Users\lishuai29\.m2\repository\io\dropwizard\metrics\metrics-json\3.1.2\metrics-json-3.1.2.jar;C:\Users\lishuai29\.m2\repository\io\dropwizard\metrics\metrics-graphite\3.1.2\metrics-graphite-3.1.2.jar;C:\Users\lishuai29\.m2\repository\com\fasterxml\jackson\module\jackson-module-scala_2.11\2.6.5\jackson-module-scala_2.11-2.6.5.jar;C:\Users\lishuai29\.m2\repository\com\fasterxml\jackson\module\jackson-module-paranamer\2.6.5\jackson-module-paranamer-2.6.5.jar;C:\Users\lishuai29\.m2\repository\org\apache\ivy\ivy\2.4.0\ivy-2.4.0.jar;C:\Users\lishuai29\.m2\repository\oro\oro\2.0.8\oro-2.0.8.jar;C:\Users\lishuai29\.m2\repository\net\razorvine\pyrolite\4.9\pyrolite-4.9.jar;C:\Users\lishuai29\.m2\repository\net\sf\py4j\py4j\0.10.1\py4j-0.10.1.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-tags_2.11\2.0.0\spark-tags_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\org\scalatest\scalatest_2.11\2.2.6\scalatest_2.11-2.2.6.jar;C:\Users\lishuai29\.m2\repository\org\scala-lang\scala-library\2.11.8\scala-library-2.11.8.jar;C:\Users\lishuai29\.m2\repository\org\spark-project\spark\unused\1.0.0\unused-1.0.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-streaming-kafka-0-8_2.11\2.0.0\spark-streaming-kafka-0-8_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-streaming-flume_2.11\2.0.0\spark-streaming-flume_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-streaming-flume-sink_2.11\2.0.0\spark-streaming-flume-sink_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\flume\flume-ng-core\1.6.0\flume-ng-core-1.6.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\flume\flume-ng-configuration\1.6.0\flume-ng-configuration-1.6.0.jar;C:\Users\lishuai29\.m2\repository\commons-cli\commons-cli\1.2\commons-cli-1.2.jar;C:\Users\lishuai29\.m2\repository\org\apache\avro\avro\1.7.4\avro-1.7.4.jar;C:\Users\lishuai29\.m2\repository\com\thoughtworks\paranamer\paranamer\2.3\paranamer-2.3.jar;C:\Users\lishuai29\.m2\repository\org\apache\avro\avro-ipc\1.7.4\avro-ipc-1.7.4.jar;C:\Users\lishuai29\.m2\repository\org\apache\velocity\velocity\1.7\velocity-1.7.jar;C:\Users\lishuai29\.m2\repository\joda-time\joda-time\2.1\joda-time-2.1.jar;C:\Users\lishuai29\.m2\repository\org\mortbay\jetty\jetty\6.1.26\jetty-6.1.26.jar;C:\Users\lishuai29\.m2\repository\com\google\code\gson\gson\2.2.2\gson-2.2.2.jar;C:\Users\lishuai29\.m2\repository\org\apache\mina\mina-core\2.0.4\mina-core-2.0.4.jar;C:\Users\lishuai29\.m2\repository\org\apache\flume\flume-ng-sdk\1.6.0\flume-ng-sdk-1.6.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-mllib_2.11\2.0.0\spark-mllib_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-graphx_2.11\2.0.0\spark-graphx_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\com\github\fommil\netlib\core\1.1.2\core-1.1.2.jar;C:\Users\lishuai29\.m2\repository\net\sourceforge\f2j\arpack_combined_all\0.1\arpack_combined_all-0.1.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-mllib-local_2.11\2.0.0\spark-mllib-local_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\org\scalanlp\breeze_2.11\0.11.2\breeze_2.11-0.11.2.jar;C:\Users\lishuai29\.m2\repository\org\scalanlp\breeze-macros_2.11\0.11.2\breeze-macros_2.11-0.11.2.jar;C:\Users\lishuai29\.m2\repository\net\sf\opencsv\opencsv\2.3\opencsv-2.3.jar;C:\Users\lishuai29\.m2\repository\com\github\rwl\jtransforms\2.4.0\jtransforms-2.4.0.jar;C:\Users\lishuai29\.m2\repository\org\spire-math\spire_2.11\0.7.4\spire_2.11-0.7.4.jar;C:\Users\lishuai29\.m2\repository\org\spire-math\spire-macros_2.11\0.7.4\spire-macros_2.11-0.7.4.jar;C:\Users\lishuai29\.m2\repository\org\apache\commons\commons-math3\3.4.1\commons-math3-3.4.1.jar;C:\Users\lishuai29\.m2\repository\org\jpmml\pmml-model\1.2.15\pmml-model-1.2.15.jar;C:\Users\lishuai29\.m2\repository\org\jpmml\pmml-schema\1.2.15\pmml-schema-1.2.15.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-sql_2.11\2.0.0\spark-sql_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\com\univocity\univocity-parsers\2.1.1\univocity-parsers-2.1.1.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-sketch_2.11\2.0.0\spark-sketch_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\spark\spark-catalyst_2.11\2.0.0\spark-catalyst_2.11-2.0.0.jar;C:\Users\lishuai29\.m2\repository\org\scala-lang\scala-reflect\2.11.8\scala-reflect-2.11.8.jar;C:\Users\lishuai29\.m2\repository\org\codehaus\janino\janino\2.7.8\janino-2.7.8.jar;C:\Users\lishuai29\.m2\repository\org\codehaus\janino\commons-compiler\2.7.8\commons-compiler-2.7.8.jar;C:\Users\lishuai29\.m2\repository\org\antlr\antlr4-runtime\4.5.3\antlr4-runtime-4.5.3.jar;C:\Users\lishuai29\.m2\repository\org\apache\parquet\parquet-column\1.7.0\parquet-column-1.7.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\parquet\parquet-common\1.7.0\parquet-common-1.7.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\parquet\parquet-encoding\1.7.0\parquet-encoding-1.7.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\parquet\parquet-generator\1.7.0\parquet-generator-1.7.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\parquet\parquet-hadoop\1.7.0\parquet-hadoop-1.7.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\parquet\parquet-format\2.3.0-incubating\parquet-format-2.3.0-incubating.jar;C:\Users\lishuai29\.m2\repository\org\apache\parquet\parquet-jackson\1.7.0\parquet-jackson-1.7.0.jar;C:\Users\lishuai29\.m2\repository\org\codehaus\jackson\jackson-core-asl\1.9.11\jackson-core-asl-1.9.11.jar;C:\Users\lishuai29\.m2\repository\com\fasterxml\jackson\core\jackson-databind\2.6.5\jackson-databind-2.6.5.jar;C:\Users\lishuai29\.m2\repository\com\fasterxml\jackson\core\jackson-annotations\2.6.0\jackson-annotations-2.6.0.jar;C:\Users\lishuai29\.m2\repository\com\fasterxml\jackson\core\jackson-core\2.6.5\jackson-core-2.6.5.jar;C:\Users\lishuai29\.m2\repository\org\apache\kafka\kafka-clients\0.9.0.1\kafka-clients-0.9.0.1.jar;C:\Users\lishuai29\.m2\repository\org\slf4j\slf4j-api\1.7.6\slf4j-api-1.7.6.jar;C:\Users\lishuai29\.m2\repository\org\xerial\snappy\snappy-java\1.1.1.7\snappy-java-1.1.1.7.jar;C:\Users\lishuai29\.m2\repository\org\apache\kafka\kafka_2.11\0.9.0.1\kafka_2.11-0.9.0.1.jar;C:\Users\lishuai29\.m2\repository\com\101tec\zkclient\0.7\zkclient-0.7.jar;C:\Users\lishuai29\.m2\repository\com\yammer\metrics\metrics-core\2.2.0\metrics-core-2.2.0.jar;C:\Users\lishuai29\.m2\repository\org\scala-lang\modules\scala-xml_2.11\1.0.4\scala-xml_2.11-1.0.4.jar;C:\Users\lishuai29\.m2\repository\org\scala-lang\modules\scala-parser-combinators_2.11\1.0.4\scala-parser-combinators_2.11-1.0.4.jar;C:\Users\lishuai29\.m2\repository\net\sf\jopt-simple\jopt-simple\3.2\jopt-simple-3.2.jar;C:\Users\lishuai29\.m2\repository\org\slf4j\slf4j-log4j12\1.7.6\slf4j-log4j12-1.7.6.jar;C:\Users\lishuai29\.m2\repository\org\apache\hbase\hbase-client\1.3.0\hbase-client-1.3.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\hbase\hbase-annotations\1.3.0\hbase-annotations-1.3.0.jar;C:\Users\lishuai29\.m2\repository\commons-codec\commons-codec\1.9\commons-codec-1.9.jar;C:\Users\lishuai29\.m2\repository\commons-io\commons-io\2.4\commons-io-2.4.jar;C:\Users\lishuai29\.m2\repository\commons-lang\commons-lang\2.6\commons-lang-2.6.jar;C:\Users\lishuai29\.m2\repository\commons-logging\commons-logging\1.2\commons-logging-1.2.jar;C:\Users\lishuai29\.m2\repository\com\google\guava\guava\12.0.1\guava-12.0.1.jar;C:\Users\lishuai29\.m2\repository\com\google\protobuf\protobuf-java\2.5.0\protobuf-java-2.5.0.jar;C:\Users\lishuai29\.m2\repository\io\netty\netty-all\4.0.23.Final\netty-all-4.0.23.Final.jar;C:\Users\lishuai29\.m2\repository\org\apache\zookeeper\zookeeper\3.4.6\zookeeper-3.4.6.jar;C:\Users\lishuai29\.m2\repository\org\apache\htrace\htrace-core\3.1.0-incubating\htrace-core-3.1.0-incubating.jar;C:\Users\lishuai29\.m2\repository\org\codehaus\jackson\jackson-mapper-asl\1.9.13\jackson-mapper-asl-1.9.13.jar;C:\Users\lishuai29\.m2\repository\org\jruby\jcodings\jcodings\1.0.8\jcodings-1.0.8.jar;C:\Users\lishuai29\.m2\repository\org\jruby\joni\joni\2.1.2\joni-2.1.2.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-auth\2.5.1\hadoop-auth-2.5.1.jar;C:\Users\lishuai29\.m2\repository\org\apache\httpcomponents\httpclient\4.2.5\httpclient-4.2.5.jar;C:\Users\lishuai29\.m2\repository\org\apache\httpcomponents\httpcore\4.2.4\httpcore-4.2.4.jar;C:\Users\lishuai29\.m2\repository\org\apache\directory\server\apacheds-kerberos-codec\2.0.0-M15\apacheds-kerberos-codec-2.0.0-M15.jar;C:\Users\lishuai29\.m2\repository\org\apache\directory\server\apacheds-i18n\2.0.0-M15\apacheds-i18n-2.0.0-M15.jar;C:\Users\lishuai29\.m2\repository\org\apache\directory\api\api-asn1-api\1.0.0-M20\api-asn1-api-1.0.0-M20.jar;C:\Users\lishuai29\.m2\repository\org\apache\directory\api\api-util\1.0.0-M20\api-util-1.0.0-M20.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-common\2.5.1\hadoop-common-2.5.1.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-annotations\2.5.1\hadoop-annotations-2.5.1.jar;D:\soft-install\jdk8\lib\tools.jar;C:\Users\lishuai29\.m2\repository\xmlenc\xmlenc\0.52\xmlenc-0.52.jar;C:\Users\lishuai29\.m2\repository\commons-httpclient\commons-httpclient\3.1\commons-httpclient-3.1.jar;C:\Users\lishuai29\.m2\repository\commons-el\commons-el\1.0\commons-el-1.0.jar;C:\Users\lishuai29\.m2\repository\commons-configuration\commons-configuration\1.6\commons-configuration-1.6.jar;C:\Users\lishuai29\.m2\repository\commons-digester\commons-digester\1.8\commons-digester-1.8.jar;C:\Users\lishuai29\.m2\repository\commons-beanutils\commons-beanutils\1.7.0\commons-beanutils-1.7.0.jar;C:\Users\lishuai29\.m2\repository\commons-beanutils\commons-beanutils-core\1.8.0\commons-beanutils-core-1.8.0.jar;C:\Users\lishuai29\.m2\repository\com\jcraft\jsch\0.1.42\jsch-0.1.42.jar;C:\Users\lishuai29\.m2\repository\org\apache\commons\commons-compress\1.4.1\commons-compress-1.4.1.jar;C:\Users\lishuai29\.m2\repository\org\tukaani\xz\1.0\xz-1.0.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-mapreduce-client-core\2.5.1\hadoop-mapreduce-client-core-2.5.1.jar;C:\Users\lishuai29\.m2\repository\org\apache\hadoop\hadoop-yarn-common\2.5.1\hadoop-yarn-common-2.5.1.jar;C:\Users\lishuai29\.m2\repository\javax\xml\bind\jaxb-api\2.2.2\jaxb-api-2.2.2.jar;C:\Users\lishuai29\.m2\repository\javax\xml\stream\stax-api\1.0-2\stax-api-1.0-2.jar;C:\Users\lishuai29\.m2\repository\javax\activation\activation\1.1\activation-1.1.jar;C:\Users\lishuai29\.m2\repository\junit\junit\4.12\junit-4.12.jar;C:\Users\lishuai29\.m2\repository\org\hamcrest\hamcrest-core\1.3\hamcrest-core-1.3.jar;C:\Users\lishuai29\.m2\repository\org\apache\hbase\hbase-common\1.3.0\hbase-common-1.3.0.jar;C:\Users\lishuai29\.m2\repository\commons-collections\commons-collections\3.2.2\commons-collections-3.2.2.jar;C:\Users\lishuai29\.m2\repository\org\mortbay\jetty\jetty-util\6.1.26\jetty-util-6.1.26.jar;C:\Users\lishuai29\.m2\repository\com\github\stephenc\findbugs\findbugs-annotations\1.3.9-1\findbugs-annotations-1.3.9-1.jar;C:\Users\lishuai29\.m2\repository\log4j\log4j\1.2.17\log4j-1.2.17.jar;C:\Users\lishuai29\.m2\repository\org\apache\hbase\hbase-protocol\1.3.0\hbase-protocol-1.3.0.jar;C:\Users\lishuai29\.m2\repository\net\jpountz\lz4\lz4\1.3.0\lz4-1.3.0.jar;C:\Users\lishuai29\.m2\repository\redis\clients\jedis\2.6.1\jedis-2.6.1.jar;C:\Users\lishuai29\.m2\repository\org\apache\commons\commons-pool2\2.0\commons-pool2-2.0.jar;C:\Users\lishuai29\.m2\repository\org\codehaus\jettison\jettison\1.3.7\jettison-1.3.7.jar;C:\Users\lishuai29\.m2\repository\stax\stax-api\1.0.1\stax-api-1.0.1.jar ls.spark.WordCount
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
19/06/08 10:34:53 INFO SparkContext: Running Spark version 2.0.0
19/06/08 10:34:54 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
19/06/08 10:34:54 ERROR Shell: Failed to locate the winutils binary in the hadoop binary path
java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
	at org.apache.hadoop.util.Shell.getQualifiedBinPath(Shell.java:355)
	at org.apache.hadoop.util.Shell.getWinUtilsPath(Shell.java:370)
	at org.apache.hadoop.util.Shell.<clinit>(Shell.java:363)
	at org.apache.hadoop.util.StringUtils.<clinit>(StringUtils.java:78)
	at org.apache.hadoop.security.Groups.parseStaticMapping(Groups.java:93)
	at org.apache.hadoop.security.Groups.<init>(Groups.java:77)
	at org.apache.hadoop.security.Groups.getUserToGroupsMappingService(Groups.java:240)
	at org.apache.hadoop.security.UserGroupInformation.initialize(UserGroupInformation.java:257)
	at org.apache.hadoop.security.UserGroupInformation.ensureInitialized(UserGroupInformation.java:234)
	at org.apache.hadoop.security.UserGroupInformation.loginUserFromSubject(UserGroupInformation.java:749)
	at org.apache.hadoop.security.UserGroupInformation.getLoginUser(UserGroupInformation.java:734)
	at org.apache.hadoop.security.UserGroupInformation.getCurrentUser(UserGroupInformation.java:607)
	at org.apache.spark.util.Utils$$anonfun$getCurrentUserName$1.apply(Utils.scala:2245)
	at org.apache.spark.util.Utils$$anonfun$getCurrentUserName$1.apply(Utils.scala:2245)
	at scala.Option.getOrElse(Option.scala:121)
	at org.apache.spark.util.Utils$.getCurrentUserName(Utils.scala:2245)
	at org.apache.spark.SparkContext.<init>(SparkContext.scala:297)
	at org.apache.spark.api.java.JavaSparkContext.<init>(JavaSparkContext.scala:58)
	at ls.spark.WordCount.main(WordCount.java:22)
19/06/08 10:34:54 INFO SecurityManager: Changing view acls to: lishuai29
19/06/08 10:34:54 INFO SecurityManager: Changing modify acls to: lishuai29
19/06/08 10:34:54 INFO SecurityManager: Changing view acls groups to: 
19/06/08 10:34:54 INFO SecurityManager: Changing modify acls groups to: 
19/06/08 10:34:54 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(lishuai29); groups with view permissions: Set(); users  with modify permissions: Set(lishuai29); groups with modify permissions: Set()
19/06/08 10:34:56 INFO Utils: Successfully started service 'sparkDriver' on port 57589.
19/06/08 10:34:56 INFO SparkEnv: Registering MapOutputTracker
19/06/08 10:34:56 INFO SparkEnv: Registering BlockManagerMaster
19/06/08 10:34:56 INFO DiskBlockManager: Created local directory at C:\Users\lishuai29\AppData\Local\Temp\blockmgr-555b6b39-0cd8-4508-89a4-6dc7eecec1a9
19/06/08 10:34:56 INFO MemoryStore: MemoryStore started with capacity 850.5 MB
19/06/08 10:34:56 INFO SparkEnv: Registering OutputCommitCoordinator
19/06/08 10:34:57 INFO Utils: Successfully started service 'SparkUI' on port 4040.
19/06/08 10:34:57 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.31.54:4040
19/06/08 10:34:57 INFO Executor: Starting executor ID driver on host localhost
19/06/08 10:34:57 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 57598.
19/06/08 10:34:57 INFO NettyBlockTransferService: Server created on 192.168.31.54:57598
19/06/08 10:34:57 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.31.54, 57598)
19/06/08 10:34:57 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.31.54:57598 with 850.5 MB RAM, BlockManagerId(driver, 192.168.31.54, 57598)
19/06/08 10:34:57 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.31.54, 57598)
19/06/08 10:34:58 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 119.9 KB, free 850.4 MB)
19/06/08 10:34:58 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 13.1 KB, free 850.4 MB)
19/06/08 10:34:58 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.31.54:57598 (size: 13.1 KB, free: 850.5 MB)
19/06/08 10:34:58 INFO SparkContext: Created broadcast 0 from textFile at WordCount.java:24
19/06/08 10:34:59 INFO FileInputFormat: Total input paths to process : 1
19/06/08 10:34:59 INFO SparkContext: Starting job: foreach at WordCount.java:76
19/06/08 10:34:59 INFO DAGScheduler: Registering RDD 3 (mapToPair at WordCount.java:37)
19/06/08 10:34:59 INFO DAGScheduler: Registering RDD 5 (mapToPair at WordCount.java:55)
19/06/08 10:34:59 INFO DAGScheduler: Got job 0 (foreach at WordCount.java:76) with 1 output partitions
19/06/08 10:34:59 INFO DAGScheduler: Final stage: ResultStage 2 (foreach at WordCount.java:76)
19/06/08 10:34:59 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 1)
19/06/08 10:34:59 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 1)
19/06/08 10:34:59 INFO DAGScheduler: Submitting ShuffleMapStage 0 (MapPartitionsRDD[3] at mapToPair at WordCount.java:37), which has no missing parents
19/06/08 10:34:59 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 4.8 KB, free 850.4 MB)
19/06/08 10:34:59 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 2.7 KB, free 850.4 MB)
19/06/08 10:34:59 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.31.54:57598 (size: 2.7 KB, free: 850.5 MB)
19/06/08 10:34:59 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1012
19/06/08 10:34:59 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 0 (MapPartitionsRDD[3] at mapToPair at WordCount.java:37)
19/06/08 10:34:59 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks
19/06/08 10:34:59 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, partition 0, PROCESS_LOCAL, 5367 bytes)
19/06/08 10:34:59 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
19/06/08 10:35:00 INFO HadoopRDD: Input split: file:/D:/IdeaProjects/ls-spark/spark.txt:0+156
19/06/08 10:35:00 INFO deprecation: mapred.tip.id is deprecated. Instead, use mapreduce.task.id
19/06/08 10:35:00 INFO deprecation: mapred.task.id is deprecated. Instead, use mapreduce.task.attempt.id
19/06/08 10:35:00 INFO deprecation: mapred.task.is.map is deprecated. Instead, use mapreduce.task.ismap
19/06/08 10:35:00 INFO deprecation: mapred.task.partition is deprecated. Instead, use mapreduce.task.partition
19/06/08 10:35:00 INFO deprecation: mapred.job.id is deprecated. Instead, use mapreduce.job.id
19/06/08 10:35:00 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1561 bytes result sent to driver
19/06/08 10:35:00 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 820 ms on localhost (1/1)
19/06/08 10:35:00 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
19/06/08 10:35:00 INFO DAGScheduler: ShuffleMapStage 0 (mapToPair at WordCount.java:37) finished in 0.855 s
19/06/08 10:35:00 INFO DAGScheduler: looking for newly runnable stages
19/06/08 10:35:00 INFO DAGScheduler: running: Set()
19/06/08 10:35:00 INFO DAGScheduler: waiting: Set(ShuffleMapStage 1, ResultStage 2)
19/06/08 10:35:00 INFO DAGScheduler: failed: Set()
19/06/08 10:35:00 INFO DAGScheduler: Submitting ShuffleMapStage 1 (MapPartitionsRDD[5] at mapToPair at WordCount.java:55), which has no missing parents
19/06/08 10:35:00 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 4.2 KB, free 850.4 MB)
19/06/08 10:35:00 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 2.4 KB, free 850.4 MB)
19/06/08 10:35:00 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.31.54:57598 (size: 2.4 KB, free: 850.5 MB)
19/06/08 10:35:00 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1012
19/06/08 10:35:00 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 1 (MapPartitionsRDD[5] at mapToPair at WordCount.java:55)
19/06/08 10:35:00 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks
19/06/08 10:35:00 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1, localhost, partition 0, ANY, 5129 bytes)
19/06/08 10:35:00 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
19/06/08 10:35:00 INFO ShuffleBlockFetcherIterator: Getting 1 non-empty blocks out of 1 blocks
19/06/08 10:35:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 7 ms
19/06/08 10:35:00 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 1880 bytes result sent to driver
19/06/08 10:35:00 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 99 ms on localhost (1/1)
19/06/08 10:35:00 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
19/06/08 10:35:00 INFO DAGScheduler: ShuffleMapStage 1 (mapToPair at WordCount.java:55) finished in 0.102 s
19/06/08 10:35:00 INFO DAGScheduler: looking for newly runnable stages
19/06/08 10:35:00 INFO DAGScheduler: running: Set()
19/06/08 10:35:00 INFO DAGScheduler: waiting: Set(ResultStage 2)
19/06/08 10:35:00 INFO DAGScheduler: failed: Set()
19/06/08 10:35:00 INFO DAGScheduler: Submitting ResultStage 2 (MapPartitionsRDD[7] at mapToPair at WordCount.java:66), which has no missing parents
19/06/08 10:35:00 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 4.0 KB, free 850.4 MB)
19/06/08 10:35:00 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 2.3 KB, free 850.4 MB)
19/06/08 10:35:00 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 192.168.31.54:57598 (size: 2.3 KB, free: 850.5 MB)
19/06/08 10:35:00 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1012
19/06/08 10:35:00 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (MapPartitionsRDD[7] at mapToPair at WordCount.java:66)
19/06/08 10:35:00 INFO TaskSchedulerImpl: Adding task set 2.0 with 1 tasks
19/06/08 10:35:00 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 2, localhost, partition 0, ANY, 5140 bytes)
19/06/08 10:35:00 INFO Executor: Running task 0.0 in stage 2.0 (TID 2)
19/06/08 10:35:00 INFO ShuffleBlockFetcherIterator: Getting 1 non-empty blocks out of 1 blocks
19/06/08 10:35:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
the   4
seven   2
and   2
score   1
white   1
ago   1
two   1
nature   1
away   1
a   1
am   1
i   1
with   1
day   1
keeps   1
at   1
apple   1
cow   1
over   1
an   1
doctor   1
moon   1
jumped   1
snow   1
years   1
dwarfs   1
four   1
19/06/08 10:35:00 INFO Executor: Finished task 0.0 in stage 2.0 (TID 2). 1553 bytes result sent to driver
19/06/08 10:35:00 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 2) in 95 ms on localhost (1/1)
19/06/08 10:35:00 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
19/06/08 10:35:00 INFO DAGScheduler: ResultStage 2 (foreach at WordCount.java:76) finished in 0.098 s
19/06/08 10:35:00 INFO DAGScheduler: Job 0 finished: foreach at WordCount.java:76, took 1.533191 s
19/06/08 10:35:00 INFO SparkUI: Stopped Spark web UI at http://192.168.31.54:4040
19/06/08 10:35:00 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
19/06/08 10:35:00 INFO MemoryStore: MemoryStore cleared
19/06/08 10:35:00 INFO BlockManager: BlockManager stopped
19/06/08 10:35:00 INFO BlockManagerMaster: BlockManagerMaster stopped
19/06/08 10:35:00 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
19/06/08 10:35:00 INFO SparkContext: Successfully stopped SparkContext
19/06/08 10:35:01 INFO ShutdownHookManager: Shutdown hook called
19/06/08 10:35:01 INFO ShutdownHookManager: Deleting directory C:\Users\lishuai29\AppData\Local\Temp\spark-4fa1aac5-45e3-4a74-a5c3-1195517316b3

Process finished with exit code 0

```