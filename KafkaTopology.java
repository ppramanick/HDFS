package com.stormadvance.storm_kafka_topology;

import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import java.util.UUID;

public class KafkaTopology {
	public static void main(String[] args) {
	/*public static Properties configs = null;
	public KafkaTopology(Properties configs) {
		this.configs = configs;
	}*/
	Properties prop = new Properties();
		try {
			SyncPolicy syncPolicy = new CountSyncPolicy(10);
	                FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(127.0f, Units.MB);
			RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");
			FileNameFormat fileNameFormat = new DefaultFileNameFormat()
						.withPrefix("kafka-storm-realtime-")
						.withExtension(".csv")
						.withPath("/user/govind/kafkaToHDFS-output");
			String port = prop.getProperty(("9000"));
			String host = prop.getProperty(("master.hadoop.lan"));
			HdfsBolt hdfsbolt = new HdfsBolt()
				.withFsUrl("hdfs://master.hadoop.lan:9000")
				.withFileNameFormat(fileNameFormat)
				.withRecordFormat(format)
				.withRotationPolicy(rotationPolicy)
				.withSyncPolicy(syncPolicy);
			// zookeeper hosts for the Kafka cluster
			BrokerHosts zkHosts = new ZkHosts("localhost:2181");

			// Create the KafkaSpout configuartion
			// Second argument is the topic name
			// Third argument is the zookeepr root for Kafka
			// Fourth argument is consumer group id
			SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "kafka-storm", "",
					"t1");

			// Specify that the kafka messages are String
			// We want to consume all the first messages in the topic everytime
			// we run the topology to help in debugging. In production, this
			// property should be false
			kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
			kafkaConfig.startOffsetTime = kafka.api.OffsetRequest
					.EarliestTime();

			// Now we create the topology
			TopologyBuilder builder = new TopologyBuilder();

			// set the kafka spout class
			builder.setSpout("kafka-spout", new KafkaSpout(kafkaConfig), 1);

			// set the word and sentence bolt class
			builder.setBolt("WordBolt", new WordBolt(), 1).globalGrouping(
					"kafka-spout");
			builder.setBolt("SentenceBolt", new SentenceBolt(), 1)
					.globalGrouping("WordBolt");
			builder.setBolt("hdfs-bolt", hdfsbolt,1).shuffleGrouping("kafka-spout");

			// create an instance of LocalCluster class for executing topology
			// in local mode.
			LocalCluster cluster = new LocalCluster();
		//Defines how many worker processes have to be created for the topology in the cluster.
			Config conf = new Config();
			//String topologyName = prop.getProperty("Storm-Kafka-HDFS-Topology");
			//conf.setNumWorkers(1);
			//cluster.submitTopology("kafkaStormHDFS-Sub", conf, builder.createTopology());
			//cluster.killTopology("kafkaStormHDFS-Sub"); cluster.shutdown();
			//Config conf = new Config();
			conf.setDebug(true);
			if (args.length > 0) {
				conf.setNumWorkers(2);
				conf.setMaxSpoutPending(5000);
				StormSubmitter.submitTopology("KafkaTopology1", conf,
						builder.createTopology());

			} else {
				// Submit topology for execution
				cluster.submitTopology("KafkaTopology1", conf,
						builder.createTopology());
				System.out.println("called1");
				Thread.sleep(1000000);
				// Wait for sometime before exiting
				System.out.println("Waiting to consume from kafka");

				System.out.println("called2");
				// kill the KafkaTopology
				//StormSubmitter.killTopology(topologyName);
				cluster.killTopology("KafkaTopology1");
				System.out.println("called3");
				// shutdown the storm test cluster
				//cluster.shutdown();
			}

		} catch (Exception exception) {
			System.out.println("Thread interrupted exception : " + exception);
		}
	}
}
