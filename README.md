# scala-flow

_scala-flow_ is a lightweight library intended to make developing Google DataFlow jobs in Scala easier. The core dataflow classes are enriched to allow more idiomatic and concise Scala usage while preserving full access to the underlying Java SDK.
    
Coders for Scala primitives and collection classes have been implemented so that you can conveniently return these types from your PTransforms. In addition you can easily create coders for your own case classes.

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

`Pipeline` has been enriched with a handful of methods.

To create a `PCollection` from in-memory data use the `transform` method instead of `apply`. This method ensures that the coder is set correctly on the input data. 
 
In addition a `run` method has been added to the `POutput` type, so that you can fluently chain transforms then run your pipeline. For example:
    
```scala
val result = Pipeline.create(...)
  .transform(Create.of("foo", "bar"))
  .apply(...transforms...)
  .run()
```
  
#### Basic PCollection Methods
  
`PCollection` now has `map`, `flatMap`, `filter` and `collect` methods that each behave as you would expect.
    
Simple example:

```scala
val result = Pipeline.create(...)
  .transform(Create.of("123", "456", "789"))
  .flatMap(_.split(""))
  .map(_.toInt)
  .filter(_ < 5)
  .collect {
    case x if x % 3 == 0 => if (x % 5 == 0) "FizzBuzz" else "Fizz"
    case x if x % 5 == 0 => "Buzz"
  }
  .run()
```

#### PCollection Extras

##### Logging Side Effect

A side-effecting method `foreach` has been added in order to allow handy debug logging. This method supplies each element of the `PCollection` to it's argument then passes on the element unchanged.
For example:

```scala
val result = Pipeline(...)
  .transform(Create.of("123", "456", "789"))
  .foreach(println)
  .apply(...continue as normal...) 
```

##### Extracting Timetamps

`extractTimestamp` converts each element in the PCollection to a tuple with its corresponding timestamp. For example:
```scala
val collection: PCollection[(String, Instant)] = Pipeline.create(...)
  .transform(Create.of("foo", "bar"))
  .withTimestamps
```

##### Converting to a `KV`

The `withKey` method provides a drop in replacement for the `WithKeys` transform.

##### Merging PCollections of the same type

The `flattenWith` method is the equivalent to the `Flatten` transform, allowing collections of the same type to be merged together. For example:

```scala
val first: PCollection[String] = ...
val second: PCollection[String] = ...
val third: PCollection[String] = ...
  
val combined: PCollection[String] = first.flattenWith(second, third)
```
  
##### Naming your transforms

To provide better visualization of the Pipeline graph and to allow updating of running jobs, you can name blocks of transforms using the `transformWith` method. For example:

```scala
  val result = Pipeline.create(...)
    .transformWith("Load Resources") { _
      .apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/kinglear.txt"))
    }
    .transformWith("Split and count Words") { _
      .flatMap(_.split("\\W+").filter(_.nonEmpty).toIterable)
      .apply(Count.perElement[String])
    }
    .transformWith("Output Results") { _
      .map(kv => kv.getKey + ": " + kv.getValue)
      .apply(TextIO.Write.to("results.text"))
    }
    .run()
```
 
Under the hood this method simply converts each nested block of methods into a `PTransform` class. 

##### ParDo Escape Hatch

The `parDo` method provides an escape hatch in case none of the existing methods do what you want. Pass any arbitrary function wth a `DoFn` signature to this method and it will be converted to a `ParDo` transform. For example:
```scala
Pipeline.create(...)
  .apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/kinglear.txt"))
  .parDo { (c: DoFn[String, String]#ProcessContext) =>
    /* Do anything whatsoever here */
    c.output(...)
  }
```

#### KV Collection

Several methods have been added specifically for KV collections:

The `mapValue` and `flatMapValue` methods allow you to change the value of a `KV` pair without affecting they key. For example:
```scala
val result = Pipeline.create(...)
  .transform(Create.of("123", "456", "789")
  .withKey(_.toInt)
  .mapValue(_.split(""))
  .flatMapValue(_.mkString(".")

/* Result contains KV(123, "1.2.3"), KV(456, "4.5.6."), KV(789, "7.8.9") */  
```

In addition there are `combinePerKey`, `topPerKey` and `groupPerKey` methods that work exactly the same as the Dataflow transform equivalents. 

#### Joining KV PCollections

In order to join two or more collections of `KV` values by key you can use `coGroupByKey`, a type-safe wrapper around Dataflow's [`CoGroupByKey`](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/join/CoGroupByKey) transform.

```scala
val buyOrders: PCollection[KV[CustomerId, BuyOrder]] = ...
val sellOrders: PCollection[KV[CustomerId, SellOrder]] = ...

val allOrders: PCollection[KV[CustomerId, (Iterable[BuyOrder], Iterable[SellOrder])]] = buyOrders.coGroupByKey(sellOrders)
```

### Coders

Implicit coders for the following types have been added:
  
  * `Int`, `Long`, `Double`
  * `Option`, `Try`, `Either`
  * `Tuple2` to `Tuple22`
  * `Iterable`, `List`, `Set`, `Map`, `Array`
  
Every method mentioned above required a coder for its output type to be implicitly available. This happens by default for any of the types listed above (and also any arbitrary combination e.g. `List[Option[(Either[String, Int], Array[Double])]]`)
If you create coders for any other types then you'll need to ensure that they are available in the implicit scope somewhere.

#### Case Class Coders

You can create a custom coder for any case class containing up to 22 members using the `caseClassCoder` method. For example:
```scala
case class Foo(name: String)
case class Bar(name: String, age: Int)
case class Qux[T](value : T)

implicit val fooCoder = caseClassCoder(Foo)
implicit val barCoder = caseClassCoder(Bar)
implicit def quxCoder = caseClassCoder(Qux.apply[T] _)
```

The last line shows demonstrates how to create a coder for a generic types, this is essentially a much simpler replacement for a `CoderFactory`.

#### Serializable Coder

By default Dataflow will always try to create a `SerializableCoder` if no other suitable coder can be found. `scala-flow` provides an equivalent with the `serializableCoder` method. For example:
```scala
class Foo(val name: String) extends Serializable
 
implicit val fooCoder = serializableCoder(Foo) 
```

## Why create a Scala Dataflow library? 

There are already some existing libraries for working with Dataflow: 
  * [Apache Beam](https://beam.apache.org): Supports not only Dataflow, but also Spark, Apex and FLink. 
  * [Scio](https://github.com/spotify/scio): Spotify have developed this excellent and extensive Scala library   

We initially used Beam directly but quickly found that the complex nature of the Java API (particulary around type erasure), made Scala interop tricky. 
We then evaluated Scio, but while we were learning the complex Dataflow concepts we wanted something that was very lightweight, and that kept us very close to the API. 
Hence this library that we feel fits in a niche between the two libraries above. 

## Roadmap

* Create version of each method that accepts a name to better support updating pipelines 
* Switch underlying support to Apache Beam

## Credits

The case class coder approach was heavily inspired by [Spray Json](https://github.com/spray/spray-json), a really nice, light weight JSON parser. 

## Contributing

Bug reports and pull requests are welcome on GitHub at
https://github.com/zendesk/scala-flow/

## Copyright and license

Copyright 2017 Zendesk, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.

You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
