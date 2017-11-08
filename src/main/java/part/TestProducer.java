package part;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		// The "all" setting we have specified will result in blocking on the
		// full commit of the record, the slowest but most durable setting.
		// " 所有"設置將導致紀錄的完整提交阻塞，最慢的，但最持久的設置
		props.put("acks", "all");
		// 如果請求失敗，生產者也會自動重試，即使設置成０ the producer can automatically retry.
		props.put("retries", 0);

		// The producer maintains buffers of unsent records for each partition.
		props.put("batch.size", 16384);
		// 默認立即發送，這裏這是延時毫秒數
		props.put("linger.ms", 1);
		// 生產者緩沖大小，當緩沖區耗盡後，額外的發送調用將被阻塞。時間超過max.block.ms將拋出TimeoutException
		props.put("buffer.memory", 33554432);
		// The key.serializer and value.serializer instruct how to turn the key and
		// value objects the user provides with their ProducerRecord into bytes.
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// 創建kafka的生產者類
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		// 生產者的主要方法
		// close();//Close this producer.
		// close(long timeout, TimeUnit timeUnit); //This method waits up to timeout for
		// the producer to complete the sending of all incomplete requests.
		// flush() ;所有緩存記錄被立刻發送
		for (int i = 0; i < 100; i++) {
			// 這裏平均寫入４個分區
			producer.send(new ProducerRecord<String, String>("foo", i % 4, Integer.toString(i), Integer.toString(i)));

		}
		producer.close();

	}
}
