![Sparkling](./Logo_200.png)

# Internet Archive's *_Sparkling_* Data Processing Library 

[![Scala version](https://img.shields.io/badge/Scala%20version-2.12.8-blue)](https://scala-lang.org/)
[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](./LICENSE)

## About Sparkling

Sparkling is a library based on Apache Spark with the goal to integrate all tools and algorithms we work with on our Hadoop cluster to process (temporal) web data. It can be used stand-alone, for example in combination with [Jupyter](https://jupyter.org), or as a dependency in other projects to access and work with web archive data. Sparkling should be considered continuous work in progress as it's growing with every new task, such as data extraction, derivation, and transformation requests from our partners or IA internal.

## Highlights

* **Efficient** CDX / (W)ARC loading, parsing and writing from / to HDFS, Petabox, …
* **Fast HTML processing** without expensive DOM parsing (SAX-like)
* Data / CDX **attachment** loaders and writers ( *.att / *.cdxa)
* Shell / Python integration for **computing derivations**
* Distributed **budget-aware repartitioning** (e.g., 1GB per partition / file)
* Advanced retry / timeout / **failure handling**
* **Lots of utilities** for logging, file handling, string operations, URL/SURT formatting, …
* **Spark extensions** and helpers
* **Easily configurable**, library-wide constants and settings
* …

## Build

To build a fat JAR based on the latest version of this code, simply clone this repo and run `sbt assembly` (SBT required)

## Usage

For the use with [Jupyter](https://jupyter.org) we recommend the [Almond kernel](https://almond.sh).

## Example

```scala
import org.archive.webservices.sparkling._
import org.archive.webservices.sparkling.warc._

val warcs = WarcLoader.load("/path/to/warcs/*arc.gz")
val pages = warcs.filter(_.http.contains(h => h.status == 200 && h.mime.contains("text/html")))
RddUtil.saveAsTextFile(pages.flatMap(_.url), "page_urls.txt.gz")
```

## Why and how Sparkling was built

The development of Sparkling has been driven by practical requirements.
When I ([Helge](mailto:helge@archive.org)) started working at the Archive as the Web Data Engineer of the web group in August 2018,
I also started working on this library, guided by the tasks that I was involved in, like:
* Computing statistics based on CDX data
* Extracting (W)ARCs from big web archive
* Process / derive data from web captures

For all of these, legacy code was available to me, describing the general processes.
However, many of the existing implementations were unnecessarily complicated,
consisted of a large number of independent files and tools, and were written in multiple languages (Pig, Python, Java/MR, ...),
which made them difficult to read, debug, reuse and extend.
Also, the code was not well optimized in terms of efficiency, for example, by simply scanning through all involved files from beginning to end, even if not required, without incorporating available indexes.

Therefore, I decided to review the code, understand how things work and reimplement them with a focus on simplicity and efficiency. The result of this work is Sparkling.

## Relation to ArchiveSpark

Sparkling has been inspired by the early work on [ArchiveSpark](https://github.com/internetarchive/ArchiveSpark) and some parts of the code were copied over initially. However, later, as Sparkling grew bigger and more feature-rich than ArchiveSpark, they switched sides and ArchiveSpark has now integrated Sparkling and is widely based on it. Today, ArchiveSpark can be considered a simplified and mostly declarative interface to the basic access and derivation functions with a descriptive data model and easy-to-use output format.

## License

[MIT](/LICENSE)