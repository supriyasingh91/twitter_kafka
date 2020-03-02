package com.rivier.project.demo;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import twitter4j.FilterQuery;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

@RestController
@RequestMapping(value = "/mytest")
public class MyController {

    TwitterStream twitterStream;

    @GetMapping
    public String test() {
        return "tootoo";
    }

    @GetMapping(value = "/start")
    public void startStream() throws Exception {
        final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);
        String consumerKey = "OyYww3QFZYTPBiKCnCqHSSXld";
        String consumerSecret = "gYTdT7EFkMsj6o4UP4ZNhzdFYvoWekJG0YEQf6hvmRan3eIAKs";
        String accessToken = "85306222-Wi1xJ5PapTk4chJU2PWvA9xJCfdUYD1dJD2p5HRl3";
        String accessTokenSecret = "HsAfPga4hMZ0WNpXNsh2pgHdZPJq9cJoaFWIQxi4xfNUe";
        String topicName = "tweets";
        String[] keyWords = { "coronavirus" };
        // Set twitter oAuth tokens in the configuration
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);
        // Create twitterstream using the configuration
        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                if (status.getLang() != null) {
                    System.out.println(status.getUser().getName() + " =========> " + status.getText());
                }
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + "upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        twitterStream.addListener(listener);
        // Filter keywords
        FilterQuery query = new FilterQuery().track(keyWords);
        twitterStream.filter(query);
        //twitterStream.sample();
        // Thread.sleep(5000);
        // Add Kafka producer config settings
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        int i = 0;
        int j = 0;
        // poll for new tweets in the queue. If new tweets are added, send them
        // to the topic
        while (true) {
            Status ret = queue.poll();
            if (ret == null) {
                Thread.sleep(100);
                i++;
            }
            else {
                for (HashtagEntity hashtage : ret.getHashtagEntities()) {
                    System.out.println("Tweet:" + ret);
                    System.out.println("Hashtag: " + hashtage.getText());
                    //producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(j++), hashtage.getText()));
                    producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(j++), ret.getText()));
                }
            }
        }
        //producer.close();
        //Thread.sleep(500);
        //twitterStream.shutdown();
    }
}
