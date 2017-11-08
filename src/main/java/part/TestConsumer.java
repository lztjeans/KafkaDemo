package part;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TestConsumer {

	public static void main(String[] args) {
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		System.out.println("this is the group part test 1");
		// 消費者的組id
		props.put("group.id", "GroupA");// 這裏是GroupA或者GroupB

		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");

		// 從poll(拉)的回話處理時長
		props.put("session.timeout.ms", "30000");
		// poll的數量限制
		// props.put("max.poll.records", "100");

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		// 訂閱主題列表topic
		consumer.subscribe(Arrays.asList("foo"));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				// 正常這裏應該使用線程池處理，不應該在這裏處理
				System.out.printf("offset = %d, key = %s, value = http://blog.csdn.net/likewindy/article/details/%s",
						record.offset(), record.key(), record.value() + "\n");

		}
	}

}