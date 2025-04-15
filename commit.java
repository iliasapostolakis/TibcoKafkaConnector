import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaConnectorWriteToKafka{
	private static final Logger logger = LogManager.getLogger("custom_logger");
/****** START SET/GET METHOD, DO NOT MODIFY *****/
	protected String stringMessage = "";
	protected String topicName = "";
	protected String bootstrapServersConfig = "";
	protected String securityProtocolConfig = "";
	protected String truststore_location = "";
	protected String truststore_password = "";
	protected String truststore_type = "";
	protected String keystore_location = "";
	protected String keystore_password = "";
	protected String keystore_type = "";
	protected int retry_time = 0;
	public String getstringMessage() {
		return stringMessage;
	}
	public void setstringMessage(String val) {
		stringMessage = val;
	}
	public String gettopicName() {
		return topicName;
	}
	public void settopicName(String val) {
		topicName = val;
	}
	public String getbootstrapServersConfig() {
		return bootstrapServersConfig;
	}
	public void setbootstrapServersConfig(String val) {
		bootstrapServersConfig = val;
	}
	public String getsecurityProtocolConfig() {
		return securityProtocolConfig;
	}
	public void setsecurityProtocolConfig(String val) {
		securityProtocolConfig = val;
	}
	public String gettruststore_location() {
		return truststore_location;
	}
	public void settruststore_location(String val) {
		truststore_location = val;
	}
	public String gettruststore_password() {
		return truststore_password;
	}
	public void settruststore_password(String val) {
		truststore_password = val;
	}
	public String gettruststore_type() {
		return truststore_type;
	}
	public void settruststore_type(String val) {
		truststore_type = val;
	}
	public String getkeystore_location() {
		return keystore_location;
	}
	public void setkeystore_location(String val) {
		keystore_location = val;
	}
	public String getkeystore_password() {
		return keystore_password;
	}
	public void setkeystore_password(String val) {
		keystore_password = val;
	}
	public String getkeystore_type() {
		return keystore_type;
	}
	public void setkeystore_type(String val) {
		keystore_type = val;
	}
	public int getretry_time() {
		return retry_time;
	}
	public void setretry_time(int val) {
		retry_time = val;
	}
/****** END SET/GET METHOD, DO NOT MODIFY *****/
    public KafkaConnectorWriteToKafka() {
    }
 
    public void invoke() throws Exception {
/* Available Variables: DO NOT MODIFY
	In  : String stringMessage
	In  : String topicName
	In  : String bootstrapServersConfig
	In  : String securityProtocolConfig
	In  : String truststore_location
	In  : String truststore_password
	In  : String truststore_type
	In  : String keystore_location
	In  : String keystore_password
	In  : String keystore_type
	In  : int retry_time
* Available Variables: DO NOT MODIFY *****/
		//remove debug before uat
		logger.debug("Starting invoke method");

		//debug before properties method, remove before uat
		logger.debug("Successfully retrieved Kafka properties:\n\t" + 
			"bootstrapServersConfig: " + bootstrapServersConfig + "\n\t" + 
			"topicName: " + topicName + "\n\t" + 
			"securityProtocolConfig: " + securityProtocolConfig + "\n\t" + 
			"truststore_location: " + truststore_location + "\n\t" +
			"truststore_password: " + truststore_password + "\n\t" +
			"truststore_type: " + truststore_type + "\n\t" +    
			"keystore_location: " + keystore_location + "\n\t" +    
			"keystore_password: " + keystore_password + "\n\t" +    
			"keystore_type: " + keystore_type);

		// Kafka broker and topic details
		String broker = bootstrapServersConfig;  // Replace with your Kafka broker IP
		String topic = topicName;  // Kafka topic
		// Kafka properties setup
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		//the Byte Array Serializer below should be used instead of StringSerializer.class.getName()
		//props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocolConfig);
		//ssl
		props.put("ssl.truststore.location", truststore_location);
        	props.put("ssl.truststore.password", truststore_password);
        	props.put("ssl.truststore.type", truststore_type);
		//ssl keystore
		props.put("ssl.keystore.location", keystore_location);
		props.put("ssl.keystore.password", keystore_password);
		props.put("ssl.keystore.type", keystore_type);
		//debug after properties method
		//remove this debug before going on uat
		logger.debug("Successfully configured Kafka properties:\n\t" +
			"BOOTSTRAP_SERVERS_CONFIG: " + props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) + "\n\t" +
			"KEY_SERIALIZER_CLASS_CONFIG: " + props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG) + "\n\t" +
			"VALUE_SERIALIZER_CLASS_CONFIG: " + props.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) + "\n\t" +
			"SECURITY_PROTOCOL_CONFIG: " + props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG) + "\n\t" +
			"truststore_location: " + props.getProperty("ssl.truststore.location") + "\n\t" +
			"truststore_password: " + props.getProperty("ssl.truststore.password") + "\n\t" +
			"truststore_type: " + props.getProperty("ssl.truststore.type") + "\n\t" +
			"keystore_location: " + props.getProperty("ssl.keystore.location") + "\n\t" +    
			"keystore_password: " + props.getProperty("ssl.keystore.password") + "\n\t" +    
			"keystore_type: " + props.getProperty("ssl.keystore.type"));

		// Checks if topic exists, this uses the topicExists method
		//If not used, this code can write to any given topic name even if it does not exist on Kafka client
		if (!topicExists(broker, topic)) {
			throw new Exception("Kafka topic does not exist: " + topic);
		}
	
		//Start of try catch block, in this part of the code, the Kafka producer is created and sends the messages
		try {
			// Create Kafka producer
			// The below line of code containing byte[] should be used in the scenario that we want Byte Array Serializer instead of String
			//KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
			KafkaProducer<String, String> producer = new KafkaProducer<>(props);
			//Remove this debug before uat
			logger.debug("Before trying anything");

			// Send the log message to Kafka
			//This line prints the stringMessage along with a customized text, remove before uat
			logger.debug("Log Message: \n"+ stringMessage);
			//remove before uat
			logger.debug("Creating record");
			//The next 3 lines of code reparse the stringMessage into a JSON object and back to a string called jsonMessage
 			ObjectMapper objectMapper = new ObjectMapper();
			// transform string to json object and then back to json string
			JsonNode jsonNode = objectMapper.readTree(stringMessage); 
			//only use below line of code in scenario that we want Byte Array Serializer
			//byte[] jsonMessage = objectMapper.writeValueAsBytes(jsonNode);
			String jsonMessage = objectMapper.writeValueAsString(jsonNode);
			//ProducerRecord<String, JsonNode> record = new ProducerRecord<>(topic, jsonNode);
			logger.debug("Log Message JSON: \n"+ jsonMessage);
			//only use below line of code in scenario that we want Byte Array Serializer
 		        //ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, jsonMessage);
 		        ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonMessage);

 			//remove debug before uat
			logger.debug("Record created");
			//this line of code uses Dynamic variables retry_time in seconds, if it does not succeed until then, it throws error
			producer.send(record).get(retry_time, TimeUnit.SECONDS);
			//producer.send(record);  use this code for no error handling
			//remove debug before uat
			logger.debug("Log message send to Kafka: " + topic);

			// Close producer
			producer.close();

       		 } catch (Exception e) {

            		logger.error("Error while sending log message to Kafka: ", e);
            		throw new Exception(e.getMessage(), e);
		
       		 } 
    }

 //checks if topic exists
 private boolean topicExists(String broker, String topic) throws Exception {
    Properties adminProps = new Properties();
    adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
    adminProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocolConfig);
    adminProps.put("ssl.truststore.location", truststore_location);
    adminProps.put("ssl.truststore.password", truststore_password);
    adminProps.put("ssl.truststore.type", truststore_type);
    adminProps.put("ssl.keystore.location", keystore_location);
    adminProps.put("ssl.keystore.password", keystore_password);
    adminProps.put("ssl.keystore.type", keystore_type);
 
    try (AdminClient adminClient = AdminClient.create(adminProps)) {
        ListTopicsResult topics = adminClient.listTopics();
        //Set<String> topicNames = topics.names().get(60, TimeUnit.SECONDS); // Added timeout to prevent hangs
         Set<String> topicNames = topics.names().get(); // No added timeout to prevent hangs
	//remove debug before uat
        logger.debug("Existing Kafka topics: " + topicNames);
	//this returns topicNames
        return topicNames.contains(topic);
    } catch (Exception e) {
	//remove error before uat
        logger.error("Error checking topic existence: ", e);
        throw e;
    }
 }
}
