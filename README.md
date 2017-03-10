# scala-flow

_scala-flow_ is a lightweight library intended to make developing Google DataFlow jobs in Scala easier. The core dataflow classes are enriched to allow more idiomatic and concise Scala usage while preserving full access to the underlying Java SDK.
    
Coders for Scala primitives and core classes have been implemented so that you can conveniently return these types from your PTransforms.

**Caveat:** This library is still evolving rapidly as we improve our knowledge and understanding of Dataflow, so there will be a some flux in the API as we discover and refine what works well and what doesn't.
    
As a preview of what's possible here's the eponymous MinimalWordCount example:

```scala     
Pipeline.create(...)
  .apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/kinglear.txt"))
  .flatMap(_.split("\\W+").filter(_.nonEmpty).toIterable)
  .apply(Count.perElement[String])
  .map(kv => kv.getKey + ": " + kv.getValue)
  .apply(TextIO.Write.to("results.text"))
  .run()
```
    
## Usage

#### Pipeline
 
A `registerScalaCoders` methods has been added to `Pipeline`. This adds Coders for the following types:
  
  * `Int`, `Long`, `Double`
  * `Option`, `Try`, `Either`
  * `Tuple2` to `Tuple22`

and then returns the pipeline itself so that it can be invoked fluently.

In addition a `run` method has been added to the `POutput` type, so that if you have a pipeline with a single final stage you can invoke it directly. 
    
```scala
   val result = Pipeline.create(...)
     .registerScalaCoders()
     .apply(... /* transforms*/)
     .apply(... /* more transforms*/)
     .run()
```
  
#### Basic Collections Methods
  
`map`, `flatMap`, `filter` and `collect` methods have been added to `PCollection`. Each of these methods behaves identically to the standard Scala collections, with the notable exception of `flatMap`. This method turns each elements of the input `PCollection` into an `Iterable` (possibly of a different type) that is then flattened into the output `PCollection`.
    
Simple example that puts it all together:

```scala
  val result = Pipeline.create(...)
    .registerScalaCoders()
    .apply(Create.of("123", "456", "789"))
    .flatMap(_.split(""))
    .map(_.toInt)
    .filter(_ < 5)
    .collect {
      case 1 => "One"
      case 2 => "Two"
      case 3 => "Three"
      case _ => "A suffusion of yellow"
    }
    .run()
```

#### Logging Side effect

A side-effecting method `foreach` has been added to allow handy debug logging. This method applies it's argument to each element of the `PCollection` then passes it on unchanged.
For example:

```scala
  val result = Pipeline(...)
    .apply(Create.of("123", "456", "789"))
    .foreach(println) // Local logging only - better to use Log4j for performance
    .apply(... /*continue as normal*/) 
```

#### Case Class Coders

You can create a custom coder for your case class using the `registerCaseClass` method, for case classes with up to 22 members. 

For example:
```scala
case class Foo(name: String)
case class Bar(name: String, age: Int)

val pipeline = Pipeline(...)
  .registerCaseClass(Foo)
  .registerCaseClass(Bar)

```

##### Note:

Since case classes implement `Serializable` it isn't strictly needed to always register your classes. By default DataFlow will use the `SerializableCoder`. 
However it's useful for the following cases:

* You wish to use a case class as a `KV` key. Serializable classes are not considered deterministic and are not allowed as keys.
* You wish to have non-serializable values in your case class. As serializable classes must themselves only recursively contain serializable members, this limits the types your case class can contain.  
* The Java serialization performance is inadequate

## Why create a Scala Dataflow library? 

There are already some existing libraries for working with Dataflow: 
  * [Apache Beam](https://beam.apache.org): Supports not only Dataflow, but also Spark, Apex and FLink. 
  * [Scio](https://github.com/spotify/scio): Spotify have developed this excellent and extensive Scala library   

We initially used Beam directly but quickly found that the complex nature of the Java API (particulary around type erasure), made Scala interop tricky. 
We then evaluated Scio, but while we were learning the complex Dataflow concepts we wanted something that was very lightweight, and that kept us very close to the API. 
Hence this library that we feel fits in a niche between the two libraries above. 

## Roadmap

* Add Coders for further common Scala types, List, Seq, Map
* General solution for case class coders
* Switch underlying support to Apache Beam

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/zendesk/scala-flow/

## Copyright and license

Copyright 2017 Zendesk, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.

You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
