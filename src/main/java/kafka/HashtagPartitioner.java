package kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.HashMap;
import java.util.Map;

public class HashtagPartitioner implements Partitioner {
    private static Map<String,Integer> hashtagToPartition;

    public void configure(Map<String, ?> configs) {
        System.out.println("Inside HashtagPartitioner.configure " + configs);
     hashtagToPartition = new HashMap<String, Integer>();
        for(Map.Entry<String,?> entry: configs.entrySet()){
            if(entry.getKey().startsWith("partitions.")){
                String keyName = entry.getKey();
                String value = (String)entry.getValue();
                System.out.println( keyName.substring(11));
                int paritionId = Integer.parseInt(keyName.substring(11));
             hashtagToPartition.put(value,paritionId);
            }
        }
    }
    
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,Cluster cluster) {
        if (hashtagToPartition.containsKey((String)key)){
            return hashtagToPartition.get((String)key);
        }else {
            int noOfPartitions = cluster.topics().size();
            return  value.hashCode()%noOfPartitions + hashtagToPartition.size() ;
        }
    }
    public void close() {}
}
