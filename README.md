# Sol: A Federated Execution Engine for Fast Distributed Computation Over Slow Networks

Sol is a federated execution engine to accelerate job executions and improve resource utilization across diverse network conditions, including in-cluster computation and federated data analytics over the Internet. To mitigate the impact of high latency, Sol proactively assigns tasks, but does so judiciously to be resilient to uncertainties. Moreover, to improve the overall resource utilization, Sol decouples communication from computation internally instead of committing resources to both aspects of a task simultaneously.

We have prototyped Sol based on [Apache Spark](https://github.com/apache/spark), wherein our modifications are to the core of Spark, so users can enjoy existing Spark-based frameworks (e.g., Spark SQL, MLlib) on Sol without migrations of their application codebase. Our evaluations on EC2 show that, compared to Apache Spark in resource-constrained networks, Sol accelerates SQL and machine learning jobs by 16.4× and 4.2× on average, while improving the resource utilization by 1.8x.

Detailed design is available in our [NSDI'20 paper](https://www.usenix.org/conference/nsdi20/presentation/lai).


## Status Quo

The source code in this repository supports interchangeable CPUs (for decoupling) inside single applications. We are testing Sol with some scheduling consistency issue, and plan to update the repository soon. 


## Building Sol

The installation of Sol follows the same of Apache Spark (2.4.0), and we attach the details below. 

Spark is built using [Apache Maven](http://maven.apache.org/).
To build Spark and its example programs, run:

    build/mvn -DskipTests clean package

You can build Spark using more than one thread by using the -T option with Maven, see ["Parallel builds in Maven 3"](https://cwiki.apache.org/confluence/display/MAVEN/Parallel+builds+in+Maven+3).
More detailed documentation is available from the project site, at
["Building Spark"](http://spark.apache.org/docs/latest/building-spark.html).

For general development tips, including info on developing Spark using an IDE, see ["Useful Developer Tools"](http://spark.apache.org/developer-tools.html).

## Configuration in Sol

For the ease of testing, users can configure several important parameters in the job profile. 

* Pipelining in **high-latency networks**
    1. ``spark.pipelining.enabled=true`` [false by default]

        It decides whether to enable pipelining.

    2. ``spark.pipelining.degree=1`` [1 by default]

        It specifies the maximum number of tasks to queue up in each site.

    3. ``spark.proactivePush.threshold=0`` [0 by default]

        It limits the maximum bytes of data shuffles in pushing to the downstream task.

    4. ``spark.prepost.enable=true`` [false by default]

        It defines whether to break down the task dependency for task early-binding.

* Decoupling in **bandwidth-constrained networks**
    1. ``spark.decoupling.enabled=true`` [false by default]

        It decides whether to enable decoupling.

    2. ``spark.decoupling.threshold=0`` [0 by default]

        It specifies the minimum bytes of data shuffles to activate task decoupling.

    3. ``spark.communicationtasks.num=1`` [1 by default]

        It specifies the number of communication tasks in data preparation, and each communication task takes 1 CPU.
    
For the rest, please refer to the [Configuration Guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

## Interactive Scala Shell

The easiest way to start using Sol is through the Scala shell:

    ./bin/spark-shell

Try the following command, which should return 1000:

    scala> sc.parallelize(1 to 1000).count()

## Benchmarks

Benchmarks used in our paper include [TPC Benchmarks](https://github.com/fanlai0990/hive-testbench) and [Intel Hibench](https://github.com/Intel-bigdata/HiBench).

## Contact  

This repository is primarily maintained by [Fan Lai](http://www-personal.umich.edu/~fanlai/), and it is joint work with Jie You, Xiangfeng Zhu, Harsha V. Madhyastha and [Mosharaf Chowdhury](https://www.mosharaf.com/) from the University of Michigan. 
