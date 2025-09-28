# Scalable All-Pairs Matching Algorithms with Apache Spark - Distributed Data Processing Project

Scala implementations for the **All-Pairs** problem in a distributed setting.  
The repository includes three variants to compute pairwise similarities between items and write the matching pairs above a configurable threshold.

---

## 📂 Contents

- `NaiveAllPairs.scala` – baseline (straightforward) implementation  
- `GroupAllPairs.scala` – grouping-based implementation to reduce comparisons  
- `OptimalGroupAllPairs.scala` – optimized grouping/pruning variant  
- `*.jar` – prebuilt runnable JARs for each algorithm  
- `instructions.txt` – usage notes

---

## 📝 Problem Overview

Given an input dataset of items represented by features (e.g., token sets or sparse vectors), compute **all item pairs** whose similarity is **≥ τ**.  
Similarities such as **Jaccard** or **Cosine** can be applied depending on the parser and function in the code.

---


## ⚙️ Build

You can either use the prebuilt `*.jar` files included in the repo or build from source.

### Using scalac (quick compile)
```bash
scalac -classpath "$SPARK_HOME/jars/*" NaiveAllPairs.scala
scalac -classpath "$SPARK_HOME/jars/*" GroupAllPairs.scala
scalac -classpath "$SPARK_HOME/jars/*" OptimalGroupAllPairs.scala
```

---

## 🛠 Requirements

To build and run the project you will need:

- **Java 8+**  
- **Scala** (2.12.x or 2.13.x recommended)  
- **Apache Spark 3.x** (local or cluster installation)  
- **sbt** (if you prefer building with sbt instead of scalac)  
- Operating System: Linux, macOS, or Windows Subsystem for Linux (WSL)  

Make sure the environment variable `$SPARK_HOME` is properly set, and add `$SPARK_HOME/bin` to your system `PATH` for easier access to `spark-submit`.
