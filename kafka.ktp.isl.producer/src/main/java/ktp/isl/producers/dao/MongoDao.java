package ktp.isl.producers.dao;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoCollection;

import java.util.concurrent.TimeUnit;

public abstract class MongoDao extends BaseDao {
    protected MongoDatabase db;
    protected MongoCollection sensorData;
    private String mongodbUri = "mongodb://godson:1nv1s1bl3@178.62.86.223:31122/ktpisl";
    private String mongodbDatabase = "ktpisl";
    private String mongodbCollection = "sensorData";

    protected MongoDao() {
        ConnectionString connString = new ConnectionString(mongodbUri);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connString).applyToConnectionPoolSettings(builder -> builder.maxWaitTime(1000, TimeUnit.MILLISECONDS))
                .retryWrites(true)
                .build();
        MongoClient mongoClient = MongoClients.create(settings);
        db = mongoClient.getDatabase(mongodbDatabase);
        sensorData = db.getCollection(mongodbCollection);
    }
}
