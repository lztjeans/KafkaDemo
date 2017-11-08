package island;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SampleProducer extends Thread {
	private final KafkaProducer<Integer, String> producer;
	private final String topic;
	private final Boolean isAsync;

	public static final String KAFKA_SERVER_URL = "od2dev5";
	public static final int KAFKA_SERVER_PORT = 6667;
	public static final String CLIENT_ID = "SampleProducer";

	public SampleProducer(String topic, Boolean isAsync) {
		Properties props = new Properties();

		// props.put("bootstrap.servers", "localhost:9092");
		props.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
		// The "all" setting we have specified will result in blocking on the
		// full commit of the record, the slowest but most durable setting.
		// " 所有"設置將導致紀錄的完整提交阻塞，最慢的，但最持久的設置
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("group.id", "GroupA");// 這裏是GroupA或者GroupB
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		// poll的數量限制
		// props.put("max.poll.records", "100");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//props.put("client.id", CLIENT_ID);

		producer = new KafkaProducer<Integer, String>(props);
		this.topic = topic;
		this.isAsync = isAsync;

	}

	public void run() {
		int messageNo = 1;
		while (messageNo <= 100) {
			String messageStr = "Message_null" + messageNo;
			long startTime = System.currentTimeMillis();
			if (isAsync) {
				producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr),
						new DemoCallBack(startTime, messageNo, messageStr));
			} else {
				try {
					//producer.send(new ProducerRecord<>(topic, messageNo, messageStr)).get();
					System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
					
					producer.send(new ProducerRecord<>(topic,messageStr)).get();
					
				} catch (Exception e) {
					e.printStackTrace();
					// handle the exception
				}
			}
			++messageNo;
		}
	}

}

class DemoCallBack implements Callback {

	private final long startTime;
	private final int key;
	private final String message;

	public DemoCallBack(long startTime, int key, String message) {
		this.startTime = startTime;
		this.key = key;
		this.message = message;
	}

	/**
	 * onCompletion method will be called when the record sent to the Kafka Server
	 * has been acknowledged.
	 * 
	 * @param metadata
	 *            The metadata contains the partition and offset of the record. Null
	 *            if an error occurred.
	 * @param exception
	 *            The exception thrown during processing of this record. Null if no
	 *            error occurred.
	 */
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		long elapsedTime = System.currentTimeMillis() - startTime;
		if (metadata != null) {
			System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), "
					+ "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
		} else {
			exception.printStackTrace();
		}
	}
}
