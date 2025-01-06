package gr.evansp;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ProducerDemo}
 */
@SuppressWarnings("nls")
public class ProducerDemo {
	/**
	 * {@link Logger}.
	 */
	private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

	/**
	 * BOOTSTRAP_SERVER
	 */
	public static final String BOOTSTRAP_SERVER = "localhost:9093";

	/**
	 * TOPIC
	 */
	public static final String TOPIC = "java_topic";

	/**
	 * Main
	 * @param args args
	 */
	public static void main(String... args) {
		sendRecord();
		sendRecordWithCallback();
	}

	private static void sendRecordWithCallback() {
		ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello World");

		Properties properties = connectionProperties();

		try(KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
			producer.send(record, (recordMetadata, e) -> {
				if (e == null) {
					logger.info("record successfully send");
				} else {
					logger.error(e.getMessage());
				}
			});
			producer.flush();
		}
	}


	private static void sendRecord() {
		ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello World");

		Properties properties = connectionProperties();

		try(KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
			producer.send(record);
			producer.flush();
		}
	}

	/**
	 * Returns the required configuration to connect to a kafka bootstrap server INSECURELY.
	 *
	 * @return Properties
	 */
	private static Properties connectionProperties() {
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return kafkaProperties;
	}
}
