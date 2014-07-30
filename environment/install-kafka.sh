apt-get update
apt-get install -y unzip  openjdk-6-jdk wget git
wget -q http://mirror.gopotato.co.uk/apache/kafka/0.8.1.1/kafka_2.8.0-0.8.1.1.tgz -O /tmp/kafka_2.8.0-0.8.1.1.tgz
tar xfz /tmp/kafka_2.8.0-0.8.1.1.tgz -C /usr/share

groupadd kafka
useradd --gid kafka --home-dir /home/kafka --create-home --shell /bin/bash kafka

chown -R kafka:kafka /usr/share/kafka_2.8.0-0.8.1.1
ln -s /usr/share/kafka_2.8.0-0.8.1.1 /usr/share/kafka

sed -i 's/zookeeper.connect=localhost:2181/zookeeper.connect=192.168.50.3:2181/' /usr/share/kafka_2.8.0-0.8.1.1/config/server.properties

