# reservoir

![Build Status](https://img.shields.io/github/workflow/status/NthPortal/reservoir/Continuous%20Integration?logo=github&style=for-the-badge)
[![Coverage Status](https://img.shields.io/coveralls/github/NthPortal/reservoir/main?logo=coveralls&style=for-the-badge)](https://coveralls.io/github/NthPortal/reservoir?branch=main)
[![Maven Central](https://img.shields.io/maven-central/v/lgbt.princess/reservoir_2.13?logo=apache-maven&style=for-the-badge)](https://mvnrepository.com/artifact/lgbt.princess/reservoir)
[![Versioning](https://img.shields.io/badge/versioning-semver%202.0.0-blue.svg?logo=semver&style=for-the-badge)](http://semver.org/spec/v2.0.0.html)
[![Docs](https://www.javadoc.io/badge2/lgbt.princess/reservoir_2.13/docs.svg?color=blue&logo=scala&style=for-the-badge)](https://www.javadoc.io/doc/lgbt.princess/reservoir_2.13)

Reservoir sampling implementation with Akka Streams support

## Add to Your sbt Build

**Scala 2.13**

```sbtshell
libraryDependencies += "lgbt.princess" %% "reservoir-core"        % "0.4.0"  // the core library supporting synchronous reservoir sampling
libraryDependencies += "lgbt.princess" %% "reservoir-akka-stream" % "0.4.0"  // the library for akka-stream operators
libraryDependencies += "lgbt.princess" %% "reservoir"             % "0.4.0"  // all parts of the library
```

## Usage

### Reservoir Sampler

```scala
import lgbt.princess.reservoir.Sampler

final case class User(id: String, displayName: String)

val sampler = Sampler[User, String](maxSampleSize = 100)(_.id)
sampler.sampleAll(onlineUsers())
val sampleIds = sampler.result()

val distinctSampler = Sampler.distinct[User, String](maxSampleSize = 100)(_.id)
distinctSampler.sampleAll(onlineUsers())
val distinctSampleIds = distinctSampler.result()
```

### Akka Stream Operator

```scala
import akka.stream.scaladsl.{Keep, Sink}
import lgbt.princess.reservoir.akkasupport.Sample

final case class User(id: String, displayName: String)

val (users1, sampleIds) = onlineUsers()
  .viaMat(Sample[User, String](maxSampleSize = 100)(_.id))(Keep.right)
  .toMat(Sink.seq)(Keep.both)
  .run()
  
val (users2, distinctSampleIds) = onlineUsers()
  .viaMat(Sample.distinct[User, String](maxSampleSize = 100)(_.id))(Keep.right)
  .toMat(Sink.seq)(Keep.both)
  .run()
```
