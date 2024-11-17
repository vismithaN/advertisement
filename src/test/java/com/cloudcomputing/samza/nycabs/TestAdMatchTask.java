package com.cloudcomputing.samza.nycabs;


import com.cloudcomputing.samza.nycabs.application.AdMatchTaskApplication;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;

public class TestAdMatchTask {
    @Test
    public void testAdMatchTask() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> confMap = new HashMap<>();
        confMap.put("stores.user-info.factory", "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        confMap.put("stores.user-info.key.serde", "integer");
        confMap.put("stores.user-info.msg.serde", "json");
        confMap.put("stores.yelp-info.factory", "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        confMap.put("stores.yelp-info.key.serde", "string");
        confMap.put("stores.yelp-info.msg.serde", "json");
        confMap.put("serializers.registry.json.class", "org.apache.samza.serializers.JsonSerdeFactory");
        confMap.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");
        confMap.put("serializers.registry.integer.class", "org.apache.samza.serializers.IntegerSerdeFactory");

        InMemorySystemDescriptor isd = new InMemorySystemDescriptor("kafka");

        InMemoryInputDescriptor imevents = isd.getInputDescriptor("events", new NoOpSerde<>());

        InMemoryOutputDescriptor outputAdStream = isd.getOutputDescriptor("ad-stream", new NoOpSerde<>());

        TestRunner
                .of(new AdMatchTaskApplication())
                .addInputStream(imevents, TestUtils.genStreamData("events"))
                .addOutputStream(outputAdStream, 1)
                .addConfig(confMap)
                .addConfig("deploy.test", "true")
                .run(Duration.ofSeconds(7));

        Assert.assertEquals(5, TestRunner.consumeStream(outputAdStream, Duration.ofSeconds(7)).get(0).size());

        ListIterator<Object> resultIter = TestRunner.consumeStream(outputAdStream, Duration.ofSeconds(7)).get(0).listIterator();

        String baseScoreTest = "{\"userId\":0,\"name\":\"Cloud Bakery\",\"storeId\":\"H4jJ7XB3CetIr1pg56CczQ\"}";
        Assert.assertEquals(mapper.readTree(baseScoreTest), resultIter.next());

        String interestTest = "{\"userId\":1,\"name\":\"Cloud Ramen\",\"storeId\":\"MJJi_5tGkWYI1VReTjhCCA\"}";
        Assert.assertEquals(mapper.readTree(interestTest), resultIter.next());

        String affordTest = "{\"userId\":2,\"name\":\"Luxury Cloud Bakery\",\"storeId\":\"H4jJ7XB3CetIr1pg56Cczx\"}";
        Assert.assertEquals(mapper.readTree(affordTest), resultIter.next());

        String updateStatusTest = "{\"userId\":3,\"name\":\"Cloud Cafe\",\"storeId\":\"MJJi_5tGkWYI1VReTjhCsx\"}";
        Assert.assertEquals(mapper.readTree(updateStatusTest), resultIter.next());

        String ageTest = "{\"userId\":4,\"name\":\"Cloud Bakery II\",\"storeId\":\"H4jJ7XB3CetIr1pg56Cczz\"}";
        Assert.assertEquals(mapper.readTree(ageTest), resultIter.next());
    }

    private static double distance(double lat1, double lon1, double lat2, double lon2) {
        if ((lat1 == lat2) && (lon1 == lon2)) {
            return 0;
        } else {
            double theta = lon1 - lon2;
            double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2))
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
            dist = Math.acos(dist);
            dist = Math.toDegrees(dist);
            dist = dist * 60 * 1.1515;
            return (dist);
        }
    }
}
