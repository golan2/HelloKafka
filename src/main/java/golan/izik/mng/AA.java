package golan.izik.mng;

/**
 * Created by golaniz on 12/02/2016.
 */
public class AA {
//    private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
//        PartitionMetadata returnMetaData = null;
//        loop:
//        for (String seed : a_seedBrokers) {
//            MySimpleConsumer consumer = null;
//            try {
//                consumer = new MySimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");
//
//                List<String> topics = Collections.singletonList(a_topic);
//                TopicMetadataRequest req = new TopicMetadataRequest(topics);
//                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
//
//                List<TopicMetadata> metaData = resp.topicsMetadata();
//
//                for (TopicMetadata item : metaData) {
//                    for (PartitionMetadata part : item.partitionsMetadata()) {
//                        if (part.partitionId() == a_partition) {
//                            returnMetaData = part;
//                            break loop;
//                        }
//                    }
//                }
//            } catch (Exception e) {
//                Utils.consolog("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
//                        + ", " + a_partition + "] Reason: " + e);
//            } finally {
//                if (consumer != null) consumer.close();
//            }
//        }
//        if (returnMetaData != null) {
//            m_replicaBrokers.clear();
//            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
//                m_replicaBrokers.add(replica.host());
//            }
//        }
//        return returnMetaData;
//    }
}
