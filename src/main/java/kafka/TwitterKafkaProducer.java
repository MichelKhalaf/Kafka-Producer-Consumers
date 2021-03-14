package kafka;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.*;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.json.JSONException;
import org.json.JSONObject;

class twitter {
	private final static HashMap<String, Integer> hashtags = new HashMap<String, Integer>();
	private static final String topic = "my-replicated-twitter";
	private static String consumerKey = "VCCLCqtcBkb8nj70wUagVMXrW";
	private static String consumerSecret = "hyy1aZOSqjyPLh4KG6jvMQhjSsHwnxZV9TCEj7MCFltKVn2pV8";
	private static String token = "1665906775-OXpWPPFy0rJG9ZB6wppmqLHFW16WFwkTOSKrJkF";
	private static String secret = "fCZXcdkKgGumKaAYqlZTvhXzGImwuxZN4uPIvRJ5k7NKd";

	public static void run(String[] args, String hashtag) throws InterruptedException {
		
		
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(Lists.newArrayList("twitterapi",hashtag));

		Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
				secret);
	
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		client.connect();
		
		for (int msgRead = 0; msgRead < 25; msgRead++) {
			String message = queue.take();
			JSONObject jsnobject;
			try {
				jsnobject = new JSONObject(message);
				message = "Name: " + ((JSONObject) jsnobject.get("user")).get("name") + "\nLocation: "+ 
						((JSONObject) jsnobject.get("user")).get("location") + "\nCreated at: " + jsnobject.get("created_at") + "\n";
						if (jsnobject.has("quoted_status")){
							if (((JSONObject)jsnobject.get("quoted_status")).has("extended_tweet")) {
							message += "Tweeted: " +((JSONObject)((JSONObject)jsnobject.get("quoted_status")).get("extended_tweet")).get("full_text") 
										+"\nRetweets:" + ((JSONObject)jsnobject.get("quoted_status")).get("retweet_count")+"\n";
						}
						}
						else if (jsnobject.has("retweeted_status")) {
							if (((JSONObject)jsnobject.get("retweeted_status")).has("extended_tweet")) {
							message += "Retweeted: " +((JSONObject)((JSONObject)jsnobject.get("retweeted_status")).get("extended_tweet")).get("full_text") +"\n";
							}
						}
						else message += "Tweet: " +jsnobject.get("text") +"\nRetweets: " +jsnobject.get("retweet_count")+"\n" ;
						
						
						
			} catch (JSONException e) {
				e.printStackTrace();
			}
			if (message != null)
			{ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,hashtag,message);
			TwitterKafkaProducer.producer.send(record);
			System.out.println(record.key());
		}
			
		else {msgRead-=1;}
		}
		client.stop();
	}	
		

		
	

	public static HashMap<String, Integer> getHashtags(){
		return hashtags;
	}
	
}
class threads extends Thread
{
	private String[] arg;
	private String hatag;
	threads(String[] args, String hashtag)
	{
		arg=args;
		hatag=hashtag;
	}
	public void run() {
		try {
			twitter.run(arg,hatag);
		}catch (InterruptedException e) {
			
		}
	}
}
public class TwitterKafkaProducer
{
	public static org.apache.kafka.clients.producer.Producer<String,String> producer;
	public static void main(String[] args) throws InterruptedException {
		Properties configProperties = new Properties();
	    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092,localhost:9093,localhost:9094");
	    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
		configProperties.put("client.id","camus");
		configProperties.put("acks","all");
		configProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,HashtagPartitioner.class.getCanonicalName());
		
		configProperties.put("partitions.0",args[0]);
		configProperties.put("partitions.1",args[1]);
		configProperties.put("partitions.2",args[2]);
		 
	    producer = new KafkaProducer<String,String>(configProperties);
		
		if (args.length != 3) {
            System.err.printf("Enter three hashtags\n",
                    twitter.class.getSimpleName());
            System.exit(-1);
        }
		threads T1= new threads(args,args[0]);
		T1.start();
		threads T2= new threads(args,args[1]);
		T2.start();
		threads T3= new threads(args,args[2]);
		T3.start();
		T1.join();
		T2.join();
		T3.join();
		producer.close();
		
	}
}

