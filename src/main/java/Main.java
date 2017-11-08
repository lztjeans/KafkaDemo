import java.util.Arrays;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import island.KafkaOperation;
import island.SampleProducer;


public class Main {
	
	

	private static String topic = "testTopic";

	public static void main(String[] arg) {
//		System.out.println("produce start work!");
//		produceImpl();
//		System.out.println("produce end work!");
		
		System.out.println("consumer start work!");
		consumerImpl();
		System.out.println("consumer end work!");		
		
//		boolean isAsync = false;
//		SampleProducer producerThread = new SampleProducer(topic, isAsync);
//		producerThread.start();
		
//		Consumer consumerThread = new Consumer("testTopic");
//        consumerThread.start();
		
	}

	public static void produceImpl() {
		// 創建kafka的生產者類
		// Producer<String, String> producer = new KafkaProducer<String, String>(props);
		Producer<String, String> producer = KafkaOperation.getProducer(null);
		// 生產者的主要方法
		// close();//Close this producer.
		// close(long timeout, TimeUnit timeUnit); //This method waits up to
		// timeout for the producer to complete the sending of all incomplete
		// requests.
		// flush() ;所有緩存紀錄立刻發送
		for (int i = 101; i < 10000; i++) {
			String topic = "test1";
			Integer partition = i%3;
			String key = "k"+i;
			String value = "v"+i;
			//這裡平均寫入4個分區
//			producer.send(new ProducerRecord<String, String>("test1", i % 3, Integer.toString(i), Integer.toString(i)));

			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, partition, key, value);
			producer.send(producerRecord);
			
		}
		producer.close();
	}

	public static void consumerImpl() {
//		String topic = "test1";
		// KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		KafkaConsumer<String, String> consumer = KafkaOperation.getConsumer("");
		//訂閱主題列表topic
		consumer.subscribe(Arrays.asList(topic));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				// 正常這裡應該使用thread pool ,不應該在這裡處理
				System.out.printf("offset = %d, key = %s, value = http://blog.csdn.net/likewindy/article/details/%s",
						record.offset(), record.key(), record.value() + "\n");
		}
	}

}
