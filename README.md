trident-tutorial
================

A practical Storm Trident tutorial

This tutorial builds on [Pere Ferrera][1]'s excellent [material][2] for the [Trident hackaton@Big Data Beers #4 in Berlin][3]. The vagrant setup is based on Taylor Goetz's [contribution][6]. The Hazelcast state code is based on wurstmeister's [code][7].

[1]:https://github.com/pereferrera
[2]:https://github.com/pereferrera/trident-hackaton
[3]:http://www.meetup.com/Big-Data-Beers/events/112226662/
[6]:https://github.com/ptgoetz/storm-vagrant
[7]:https://github.com/wurstmeister/storm-kafka-0.8-plus-test

Have a look at the accompanying [slides][4] as well.

[4]:http://htmlpreview.github.io/?https://rawgithub.com/mischat/trident-tutorial/blob/master/slides/index.html#(4)

## How this tutorial is structured
* Go through Part*.java to learn about the basics of Trident
* Implement your own topology using Skeleton.java, or have a look at other examples

```
├── environment                                ------ Vagrant resources to simulate a Storm cluster locally 
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


## Before you run the tutorials
 1. Install Java 1.6 and Maven 3 (these versions are recommended, but you can also use Java 1.7 and/or Maven 2) 
 2. Clone this repo (if you don't have git, you can also download the source as zip file and extract it)
 3. Go to the project folder and execute `mvn clean package`, and see if the build succeeds

## Going through Part*.java
These classes are primarily meant to be read, but you can run them as well. Before you run the main method, you should comment out all streams except the one you are interested in (otherwise there will be lots of output)

## Running the Skeleton and examples
These toplogies expect a Kafka spout that streams tweets. The Kafka spout needs a Kafka queue. There is a utility class called `Tweetingestor` which starts a local Kafka broker, connects to twitter and publishes tweets. To use this class however, you must provide a valid twitter access token in `twitter4j.properties` file.   
To do that,
 1. Go to https://dev.twitter.com and register
 2. Create an application and obtain a consumer key, consumer secret, access token and an access secret
 3. Copy `twitter4j.properties.template` as `twitter4j.properties` and replcace the `*******` with real credentials
 4. After that, execute

```bash
java -cp target/trident-tutorial-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
        tutorial.storm.trident.Skeleton
```
    

## Running a Storm cluster
You can simulate a multi-machine Storm cluster on your local machine. To do this, first install [vagrant][5]. Then, install its host manager plugin by executing

```
$ vagrant plugin install vagrant-hostmanager
```  

Finally, go to `./environment` and execute `vagrant up`. It will take a while to download necessary resources, and you will be asked for root password as it edits the host file.

You can list the vagrant VMs as follows:
```
$ vagrant status
Current machine states:

util                      running (virtualbox)
zookeeper                 running (virtualbox)
nimbus                    running (virtualbox)
supervisor1               running (virtualbox)
supervisor2               running (virtualbox)
```

The vagrant configuration file is configured to forward standard storm/kafka/zookeeper ports from the host machine (i.e. your machine) to the appropriate guest VM, so that you can e.g. look at Storm UI by simply navigating to `http://localhost:8080`. You can ssh into these machines by doing e.g. `vagrant ssh nimbus` and take a look. Storm components are run by the `storm` user.


[5]:http://www.vagrantup.com/


## Running example topologies on the Storm cluster
### Start the Kafka server
Currently you have to start the Kafka server manually. To do so, ssh into the `util` server like so:
```
vagrant ssh util
```
Then, start the Kafka server in the background
```
sudo /bin/su - kafka -c "/usr/share/kafka_2.8.0-0.8.1.1/bin/kafka-server-start.sh -daemon /usr/share/kafka_2.8.0-0.8.1.1/config/server.properties"
```
