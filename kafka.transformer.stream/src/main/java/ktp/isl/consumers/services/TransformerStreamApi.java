package ktp.isl.consumers.services;

import com.google.gson.Gson;

import java.util.*;
import java.util.function.Consumer;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoCollection;
import ktp.isl.consumers.dao.MongoDao;
import ktp.isl.consumers.models.SensorBean;
import ktp.isl.consumers.models.SensorCodec;
import ktp.isl.consumers.models.SensorDataBean;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class TransformerStreamApi extends MongoDao {


    private  Gson jsonParser = new Gson();
    private  String topic = "sensor_data";

    public static void main(String[] args) {
        new TransformerStreamApi().run();

    }

    private void run(){String bootstrapServer = "localhost:9092";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServer);
        properties.setProperty("application.id", "transformer-streams");
        properties.setProperty("default.key.serde", StringSerde.class.getName());
        properties.setProperty("default.value.serde", StringSerde.class.getName());
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputTopic = streamsBuilder.stream(topic);
        //inputTopic.flatMapValues(<>)

        // KStream<String, String> filteredStream = inputTopic.filter((k, jsonTweet) -> {        //    return extractUserFollowersInTweet(jsonTweet) > 10000;
        //});
        //  filteredStream.to("important_tweets");
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();

    }

    private SensorDataBean extractUserFollowersInTweet(String recordData) {
        try {
            return jsonParser.fromJson(recordData,SensorDataBean.class);
        } catch (NullPointerException var2) {
            return null;
        }
    }

    private Map<String, SensorBean> getSensorMeta() {
        SensorCodec sensorCodec = new SensorCodec();
        CodecRegistry codecRegistry =
                fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), fromCodecs(sensorCodec));
        Bson queryFilter =
                and(
                        eq("activated", 1)
                        // ,in("sensor_id", sensorsList)
                );
        MongoCollection<SensorBean> sensors = db.getCollection("devices", SensorBean.class).withCodecRegistry(codecRegistry);
        List<SensorBean> sensorBeans = new ArrayList<>();
        sensors.find(queryFilter).into(sensorBeans);
        Map<String,SensorBean> sensorBeanMap = new HashMap<String,SensorBean>();
        Consumer<SensorBean> beanConsumer = (p)->sensorBeanMap.put(p.getSensorId()+"",p);
        sensorBeans.forEach(res->beanConsumer.accept(res));
        return  sensorBeanMap;
    }
}
