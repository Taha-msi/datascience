package ktp.isl.consumers.dao;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import ktp.isl.consumers.models.SensorBean;
import ktp.isl.consumers.models.SensorCodec;
import org.bson.codecs.configuration.CodecRegistry;

import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;


public class MongoDao {
    protected MongoDatabase db;
    protected MongoCollection sensorData;
    protected MongoCollection<SensorBean> devicesList;

    private String mongodbUri = "mongodb://godson:godson123@10.1.0.162:27017/ktpisl";
    private String mongodbDatabase = "ktpisl";
    private String sensorCollection = "sensorDataLakeTest";
    private String deviceCollection = "devices";

    protected MongoDao() {
        ConnectionString connString = new ConnectionString(mongodbUri);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connString)
                .retryWrites(true)
                .build();
        MongoClient mongoClient = MongoClients.create(settings);
        db = mongoClient.getDatabase(mongodbDatabase);
        sensorData = db.getCollection(sensorCollection);
        SensorCodec sensorCodec = new SensorCodec();
        CodecRegistry codecRegistry =
                fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), fromCodecs(sensorCodec));
        devicesList = db.getCollection("devices", SensorBean.class).withCodecRegistry(codecRegistry);
    }
}
