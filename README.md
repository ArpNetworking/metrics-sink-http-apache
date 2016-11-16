Metrics HTTP Sink
=================

<a href="https://raw.githubusercontent.com/ArpNetworking/metrics-sink-http-apache/master/LICENSE">
    <img src="https://img.shields.io/hexpm/l/plug.svg"
         alt="License: Apache 2">
</a>
<a href="https://travis-ci.org/ArpNetworking/metrics-sink-http-apache/">
    <img src="https://travis-ci.org/ArpNetworking/metrics-sink-http-apache.png?branch=master"
         alt="Travis Build">
</a>
<a href="http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.arpnetworking.metrics%22%20a%3A%22sink-http-apache%22">
    <img src="https://img.shields.io/maven-central/v/com.arpnetworking.metrics/sink-http-apache.svg"
         alt="Maven Artifact">
</a>

Common utility methods and classes.

Usage
-----

### Add Dependency

Determine the latest version of the library in [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.arpnetworking.metrics%22%20a%3A%22sink-http-apache%22).

#### Maven

Add a dependency to your pom:

```xml
<dependency>
    <groupId>com.arpnetworking.metrics</groupId>
    <artifactId>sink-http-apache</artifactId>
    <version>VERSION</version>
</dependency>
```

The Maven Central repository is included by default.

#### Gradle

Add a dependency to your build.gradle:

    compile group: 'com.arpnetworking.metrics', name: 'metrics-sink-http-apache', version: 'VERSION'

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
    "com.arpnetworking.metrics" % "metrics-sink-http-apache" % "VERSION"
)
```

The Maven Central repository is included by default.

Building
--------

Prerequisites:
* [JDK8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (Or Invoke with JDKW)

Building:

    commons> ./mvnw verify

To use the local version you must first install it locally:

    commons> ./mvnw install

You can determine the version of the local build from the pom file.  Using the local version is intended only for testing or development.

You may also need to add the local repository to your build in order to pick-up the local version:

* Maven - Included by default.
* Gradle - Add *mavenLocal()* to *build.gradle* in the *repositories* block.
* SBT - Add *resolvers += Resolver.mavenLocal* into *project/plugins.sbt*.

License
-------

Published under Apache Software License 2.0, see LICENSE

&copy; Inscope Metrics Inc., 2016
