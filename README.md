trident-tutorial
================

A practical Storm Trident tutorial

This tutorial builds on [Pere Ferrera][1]'s excellent [material][2] for the [Trident hackaton@Big Data Beers #4 in Berlin][3]

[1]:https://github.com/pereferrera
[2]:https://github.com/pereferrera/trident-hackaton
[3]:http://www.meetup.com/Big-Data-Beers/events/112226662/

Have a look at the accompanying [slides][4] as well.

[4]:http://htmlpreview.github.io/?https://rawgithub.com/mischat/trident-tutorial/blob/master/slides/index.html#(4)

## How this tutorial is structured
* Go through Part*.java to learn about the basics of Trident
* Implement your own topology using Skeleton.java, or have a look at other examples

```
├── src
    └── main
        ├── java
        │   └── tutorial
        │       └── storm
        │           ├── trident
        │           |    ├── example           ------ Complete examples
        │           |    ├── operations        ------ Functions and filters that is used in the tutorial/examples
        │           |    └── testutil          ------ Test utility classes (e.g test data generators)
        |           |        └── TweetIngestor ------ Creates a local Kafka broker that streams twitter public stream
        |           ├── Part*.java             ------ Illustrates usage of Trident step by step.
        |           └── Skeleton.java          ------ Stub for writing your own topology
        └── resources
            └── tutorial
                └── storm
                    └── trident
                        └── testutil      ------ Contains test data and config files
```


## How to run the tutorial
 1. Install Java 1.6 and Maven 3 (these versions are recommended, but you can also use Java 1.7 and/or Maven 2) 
 2. Clone this repo (if you don't have git, you can also download the source as zip file and extract it)
 3. Go to the project folder and execute `mvn clean package` 
 4. Run 

```bash
java -cp target/trident-tutorial-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
        tutorial.storm.trident.example.TopHashtagAnalysis \
        ec2-54-216-194-46.eu-west-1.compute.amazonaws.com:12000
```
    

    
