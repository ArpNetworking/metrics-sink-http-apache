Metrics Client Apache HTTP Sink
===============================

<a href="https://raw.githubusercontent.com/ArpNetworking/metrics-apache-http-sink-extra/master/LICENSE">
    <img src="https://img.shields.io/hexpm/l/plug.svg"
         alt="License: Apache 2">
</a>
<a href="https://travis-ci.org/ArpNetworking/metrics-apache-http-sink-extra/">
    <img src="https://travis-ci.org/ArpNetworking/metrics-apache-http-sink-extra.png?branch=master"
         alt="Travis Build">
</a>
<a href="http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.arpnetworking.metrics.extras%22%20a%3A%22apache-http-sink-extra%22">
    <img src="https://img.shields.io/maven-central/v/com.arpnetworking.metrics.extras/apache-http-sink-extra.svg"
         alt="Maven Artifact">
</a>
<a href="http://www.javadoc.io/doc/com.arpnetworking.metrics.extras/apache-http-sink-extra">
    <img src="http://www.javadoc.io/badge/com.arpnetworking.metrics.extras/apache-http-sink-extra.svg"
         alt="Javadocs">
</a>

Apache HTTP sink for metrics client.

Usage
-----

### Add Dependency

Determine the latest version of the library in [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.arpnetworking.metrics.extras%22%20a%3A%22apache-http-sink-extra%22).

#### Maven

Add a dependency to your pom:

```xml
<dependency>
    <groupId>com.arpnetworking.metrics.extras</groupId>
    <artifactId>apache-http-sink-extra</artifactId>
    <version>VERSION</version>
</dependency>
```

The Maven Central repository is included by default.

#### Gradle

Add a dependency to your build.gradle:

    compile group: 'com.arpnetworking.metrics.extras', name: 'apache-http-sink-extra', version: 'VERSION'

Add the Maven Central Repository into your *build.gradle*:

```groovy
repositories {
    mavenCentral()
}
```

#### SBT

Add a dependency to your project/Build.scala:

```scala
val appDependencies = Seq(
    "com.arpnetworking.metrics.extras" % "apache-http-sink-extra" % "VERSION"
)
```

The Maven Central repository is included by default.

### Set as Sink on MetricsFactory

To override the default sink on the _MetricsFactory_ do the following:

```java
final MetricsFactory metricsFactory = new TsdMetricsFactory.Builder()
        .setSinks(Collections.singletonList(new ApacheHttpSink.Builder().build())
        .build();
```

In most cases the default arguments are sufficien; however, you may also customize the _ApacheHttpSink_ like this:

```java
final MetricsFactory metricsFactory = new TsdMetricsFactory.Builder()
        .setSinks(Collections.singletonList(
                new ApacheHttpSink.Builder()
                        .setMaxBatchSize(1000)
                        .setEmptyQueueInterval(Duration.ofMillis(1000))
                        .setParallelism(4)
                        .setUri(URI.create("http://remote-mad.example.com")
                        .setBufferSize(100000)
                        .build())
        .build();
```

For more information on configuring _MetricsFactory_ please see [metrics-client-java](https://github.com/ArpNetworking/metrics-client-java).

Building
--------

Prerequisites:
* [JDK8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (Or Invoke with JDKW)

Building:

    metrics-apache-http-sink-extra> ./mvnw verify

To use the local version you must first install it locally:

    metrics-apache-http-sink-extra> ./mvnw install

You can determine the version of the local build from the pom file.  Using the local version is intended only for testing or development.

You may also need to add the local repository to your build in order to pick-up the local version:

* Maven - Included by default.
* Gradle - Add *mavenLocal()* to *build.gradle* in the *repositories* block.
* SBT - Add *resolvers += Resolver.mavenLocal* into *project/plugins.sbt*.

License
-------

Published under Apache Software License 2.0, see LICENSE

&copy; Inscope Metrics Inc., 2016
