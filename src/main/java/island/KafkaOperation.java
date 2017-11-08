package island;

import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;



public class KafkaOperation {

	public static Properties props = null;

	static {

		
	}

	private static void initProps() {
		props = new Properties();

//      props.put("bootstrap.servers", "localhost:9092");
		props.put("bootstrap.servers", "10.1.100.104:6667");
		// The "all" setting we have specified will result in blocking on the
		// full commit of the record, the slowest but most durable setting.
		// " 所有"設置將導致紀錄的完整提交阻塞，最慢的，但最持久的設置
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("group.id", "GroupA");// 這裏是GroupA或者GroupB
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		// poll的數量限制
		// props.put("max.poll.records", "100");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	}
	
	private static void initProps(String configPath) {
		props = PropertiesUtil.getProperties(configPath);
	}


	public static void setConsumerGroupID(String grpID) {
		if (props != null) {
			props.put("group.id", grpID);
		}
	}

	public static KafkaConsumer<String, String> getConsumer(String configPath) {
		if (null == configPath || "".equals(configPath)) {
			initProps();
		} else {
			initProps(configPath);
		}
		return new KafkaConsumer<String, String>(props);
	}

	public static Producer<String, String> getProducer(String configPath) {
		if (null == configPath || "".equals(configPath)) {
			initProps();
		} else {
			initProps(configPath);
		}
		return new KafkaProducer<String, String>(props);
	}

}
/*
	public static HdfsOperater getOperater() throws CUBException {
		try {
			Properties prop = PropertiesUtil.getProperties(PropertiesUtil.HDCOS_CONFIG_PATH);
			Set<Object> propkeys =  prop.keySet();
			Configuration conf = new Configuration();
			for(Object opropkey : propkeys) {
				String propkey = StringUtil.parseNull(opropkey);
				if(!"".equals(propkey) && propkey.startsWith("hdfs.")) {
					String propValue = StringUtil.parseNull(prop.getProperty(propkey));
					String confKey = propkey.replaceFirst("hdfs.", "");
					conf.set(confKey , propValue);
				}
			}
			conf.set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(StringUtil.parseNull(prop.getProperty(kuser)) , StringUtil.parseNull(prop.getProperty(kkeytab)));

			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HDFS_001 , HdfsOperater.class.getName());
		}
		return new HdfsOperater();
	}
	
	
	/////////////////
	public static Properties getProperties(String propertiesPath) {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream(propertiesPath);
//			input = PropertiesUtil.class.getResourceAsStream(propertiesPath);
			prop.load(input);
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return prop;
	}
*/
