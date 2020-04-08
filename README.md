# reservoir

[![Build Status](https://travis-ci.org/NthPortal/reservoir.svg?branch=master)](https://travis-ci.org/NthPortal/reservoir)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/lgbt.princess/reservoir-core_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/lgbt.princess/reservoir-core_2.13)

Reservoir sampling implementation with akka-streams support

## Add to Your sbt Build

**Scala 2.13: core**

```sbtshell
libraryDependencies += "lgbt.princess" %% "reservoir-core" % "0.2.1"
```

**Scala 2.13: akka-streams support**

```sbtshell
libraryDependencies += "lgbt.princess" %% "reservoir-akka" % "0.2.1"
```

## Usage

### Reservoir Sampler

```scala
import lgbt.princess.reservoir.Sampler

final case class User(id: String, displayName: String)

val sampler = Sampler[User, String](maxSampleSize = 100)(_.id)
onlineUsers() foreach sampler.sample
val sampleIds = sampler.result()

val distinctSampler = Sampler.distinct[User, String](maxSampleSize = 100)(_.id)
onlineUsers() foreach distinctSampler.sample
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
