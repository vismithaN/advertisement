package com.cloudcomputing.samza.nycabs;

import com.google.common.io.Resources;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Arrays;


/**
 * Consumes the stream of events.
 * Outputs a stream which handles static file and one stream
 * and gives a stream of advertisement matches.
 */
public class AdMatchTask implements StreamTask, InitableTask {

    /*
       Define per task state here. (kv stores etc)
       READ Samza API part in Writeup to understand how to start
    */

    private KeyValueStore<Integer, Map<String, Object>> userInfo;

    private KeyValueStore<String, Map<String, Object>> yelpInfo;

    private final static Integer DURATION_MILLI = 5 * 60 * 1000;

    private final  ObjectMapper mapper = new ObjectMapper();

    private Set<String> lowCalories;

    private Set<String> energyProviders;

    private Set<String> willingTour;

    private Set<String> stressRelease;

    private Set<String> happyChoice;

    private void initSets() {
        lowCalories = new HashSet<>(Arrays.asList("seafood", "vegetarian", "vegan", "sushi"));
        energyProviders = new HashSet<>(Arrays.asList("bakeries", "ramen", "donuts", "burgers",
                "bagels", "pizza", "sandwiches", "icecream",
                "desserts", "bbq", "dimsum", "steak"));
        willingTour = new HashSet<>(Arrays.asList("parks", "museums", "newamerican", "landmarks"));
        stressRelease = new HashSet<>(Arrays.asList("coffee", "bars", "wine_bars", "cocktailbars", "lounges"));
        happyChoice = new HashSet<>(Arrays.asList("italian", "thai", "cuban", "japanese", "mideastern",
                "cajun", "tapas", "breakfast_brunch", "korean", "mediterranean",
                "vietnamese", "indpak", "southern", "latin", "greek", "mexican",
                "asianfusion", "spanish", "chinese"));
    }

    // Get store tag
    private String getTag(String cate) {
        String tag = "";
        if (happyChoice.contains(cate)) {
            tag = "happyChoice";
        } else if (stressRelease.contains(cate)) {
            tag = "stressRelease";
        } else if (willingTour.contains(cate)) {
            tag = "willingTour";
        } else if (energyProviders.contains(cate)) {
            tag = "energyProviders";
        } else if (lowCalories.contains(cate)) {
            tag = "lowCalories";
        } else {
            tag = "others";
        }
        return tag;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize kv store

        userInfo = (KeyValueStore<Integer, Map<String, Object>>) context.getTaskContext().getStore("user-info");
        yelpInfo = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext().getStore("yelp-info");

        //Initialize store tags set
        initSets();

        //Initialize static data and save them in kv store
        initialize("UserInfoData.json", "NYCstore.json");
    }

    /**
     * This function will read the static data from resources folder
     * and save data in KV store.
     * <p>
     * This is just an example, feel free to change them.
     */
    public void initialize(String userInfoFile, String businessFile) {
        List<String> userInfoRawString = AdMatchConfig.readFile(userInfoFile);
        System.out.println("Reading user info file from " + Resources.getResource(userInfoFile).toString());
        System.out.println("UserInfo raw string size: " + userInfoRawString.size());
        for (String rawString : userInfoRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
//                mapResult.put("interest", StringUtils.EMPTY);
//                mapResult.put("mood", -1);
//                mapResult.put("blood_sugar", -1);
//                mapResult.put("stress", -1);
//                mapResult.put("active", -1);
                mapResult.put("tags", getUserTag(mapResult));

                int userId = (Integer) mapResult.get("userId");
                userInfo.put(userId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse user info :" + rawString);
            }
        }

        List<String> businessRawString = AdMatchConfig.readFile(businessFile);

        System.out.println("Reading store info file from " + Resources.getResource(businessFile).toString());
        System.out.println("Store raw string size: " + businessRawString.size());

        for (String rawString : businessRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                String storeId = (String) mapResult.get("storeId");
                String cate = (String) mapResult.get("categories");
                String tag = getTag(cate);
                mapResult.put("tag", tag);
                yelpInfo.put(storeId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse store info :" + rawString);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        /*
        All the messsages are partitioned by blockId, which means the messages
        sharing the same blockId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
        */
        String incomingStream = envelope.getSystemStreamPartition().getStream();
        Map<String,Object> event = (Map<String, Object>)envelope.getMessage();

        if (incomingStream.equals(AdMatchConfig.EVENT_STREAM.getStream())) {
            // Handle Event messages
            String type = event.get("type").toString();
            if ("RIDER_STATUS".equals(type)) {
                handleRiderStatus(event);
            } else if ("RIDER_INTEREST".equals(type)) {
                handleRiderInterest(event);
            } else if ("RIDE_REQUEST".equals(type)) {
                handleRideRequest(event, collector);
            }
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    private void handleRideRequest(Map<String, Object> event, MessageCollector collector) throws IOException {
        int userId = (Integer) event.get("clientId");
        Map<String, Object> user = userInfo.get(userId);

        if (user == null) return;
        double maxScore = Double.MIN_VALUE;
        Map<String, Object> bestMatch = null;

        Set<String> userTags = tagsConversion(user);
        String userInterest = (String) user.get("interest");
        String device = (String) user.get("device");

        System.out.println("UserTags " + user.get("tags"));

        KeyValueIterator<String,Map<String,Object>> iterator = yelpInfo.all();

        while(iterator.hasNext()) {
            Map<String, Object> store = iterator.next().getValue();

            //Tags match
            String storeTag = (String) store.get("tag");
            if (!userTags.contains(storeTag)) continue;

            // Initial Score
            double score = (Integer) store.get("review_count") * (Double) store.get("rating");

            //User Interest and Store Category Match
            if (store.get("categories").equals(userInterest)) {
                score += 10;
            }

            //Price and Device match
            int priceValue = getPriceValue((String) store.get("price"));
            int deviceValue = getDeviceValue(device);
            score *= (1 - Math.abs(priceValue - deviceValue) * 0.1);

            //Distance and Age match
            score = distanceAgeMatch(store, user, event,score);

            //update max score
            if (score > maxScore) {
                maxScore = score;
                bestMatch = store;
            }
        }
        if (bestMatch != null) {
            Map<String,Object> output = new HashMap<>();
            output.put("userId", userId);
            output.put("storeId",bestMatch.get("storeId"));
            output.put("name", bestMatch.get("name"));
            collector.send(new OutgoingMessageEnvelope(AdMatchConfig.AD_STREAM,
                    mapper.readTree(mapper.writeValueAsString(output))));
        }

    }

    private Set<String> tagsConversion(Map<String, Object> user) {
        Set<String> userTags;
        Object tagsObject = user.get("tags");

        if (tagsObject instanceof List) {
            // Convert to Set if stored as a List
            userTags = new HashSet<>((List<String>) tagsObject);
        } else if (tagsObject instanceof Set) {
            userTags = (Set<String>) tagsObject;
        } else {
            throw new IllegalStateException("Unexpected type for tags field: " + tagsObject.getClass());
        }
        return userTags;
    }

    private double distanceAgeMatch(Map<String, Object> store, Map<String, Object> user,
                                    Map<String, Object> event, double score) {
        int travelCount = (Integer) user.get("travel_count");
        int age = (Integer) user.get("age");
        double distance = calculateDistance((Double) store.get("latitude"), (Double) store.get("longitude"),
                (Double) event.get("latitude"), (Double) event.get("longitude"));

        if((travelCount > 50 || age == 20) && distance > 10)
            score *= 0.1;
        else if ((travelCount <= 50 && age > 20) && distance > 5)
            score *= 0.1;

        return score;
    }

    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
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

    private int getPriceValue(String price) {
        switch (price) {
            case "$$$$":
            case "$$$":
                return 3;
            case "$$":
                return 2;
            case "$":
                return 1;
            default:
                return 0;
        }
    }

    private int getDeviceValue(String device) {
        switch (device) {
            case "iPhone XS":
                return 3;
            case "iPhone 7":
                return 2;
            case "iPhone 5":
                return 1;
            default:
                return 0;
        }

    }




    private void handleRiderInterest(Map<String, Object> event) {
        int userId = (Integer) event.get("userId");
        Map<String,Object> userProfile =  userInfo.get(userId);
        if (userProfile == null) return;

        //Update only of Duration > 5sec
        int duration = (Integer) event.get("duration");
        if (duration > DURATION_MILLI) {
            userProfile.put("interest", event.get("interest"));
            userInfo.put(userId, userProfile);
        }
    }


    private void handleRiderStatus(Map<String, Object> event) {
        int userId = (Integer) event.get("userId");
        Map<String,Object> user =  userInfo.get(userId);
        if (user == null) return;

        // Create tags
        Set<String> tags = getUserTag(event);

        //Update user's profile
        user.put("mood", event.get("mood"));
        user.put("blood_sugar", event.get("blood_sugar"));
        user.put("stress", event.get("stress"));
        user.put("active", event.get("active"));
        user.put("tags", tags);

        userInfo.put(userId, user);
    }

    private Set<String> getUserTag(Map<String,Object> event) {
        Set<String> tags = new HashSet<>();

        int mood = (Integer) event.get("mood");
        int bloodSugar = (Integer) event.get("blood_sugar");
        int stress = (Integer) event.get("stress");
        int active = (Integer) event.get("active");

        if (bloodSugar > 4 && mood > 6 && active == 3) tags.add("lowCalories");
        if (bloodSugar < 2 || mood < 4) tags.add("energyProviders");
        if (active == 3) tags.add("willingTour");
        if (stress > 5 || active == 1 || mood < 4) tags.add("stressRelease");
        if (mood > 6) tags.add("happyChoice");
        if (tags.isEmpty()) tags.add("others");

        return tags;
    }



}
