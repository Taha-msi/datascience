package ktp.isl.consumers.dao;

import java.util.*;
import java.util.function.Consumer;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import ktp.isl.consumers.models.SensorBean;
import ktp.isl.consumers.utils.Date;
import org.bson.Document;
import org.bson.BsonNull;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;


import com.mongodb.client.model.Field;
import org.bson.conversions.Bson;


public class SensorDataImpl extends MongoDao implements SensorDataDao {
    @Override
    public Map<Long, SensorBean> getSensorMetaData(List<Long> sensorsList) {
        Bson queryFilter =
                and(
                        eq("activated", 1)
                        , in("sensor_id", sensorsList)
                );
        List<SensorBean> sensorBeans = new ArrayList<>();
        devicesList.find(queryFilter).into(sensorBeans);
        Map<Long, SensorBean> sensorBeanMap = new HashMap<>();
        Consumer<SensorBean> beanConsumer = (p) -> sensorBeanMap.put(p.getSensorId(), p);
        sensorBeans.forEach(res -> beanConsumer.accept(res));
        return sensorBeanMap;
    }

    @Override
    public void updateSensorData(List<Long> sensorsList, java.util.Date dateString) {
        Date date = new Date();
        Bson matchFilter =
                and(
                        eq("recordDay", dateString)
                        , in("sensor_id", sensorsList)
                );
        Bson matchStage = Aggregates.match(matchFilter);
        doAggregate(matchStage);
    }

    @Override
    public void updateSensorData(List<Long> sensorsList) {
        Bson matchFilter = in("sensor_id", sensorsList);
        Bson matchStage = Aggregates.match(matchFilter);
        doAggregate(matchStage);
    }

    @Override
    public void updateSensorData() {
        Bson matchFilter = new Document();
        Bson matchStage = Aggregates.match(matchFilter);
        doAggregate(matchStage);
    }

    private void doAggregate(Bson matchStage) {
        Bson groupID = and(eq("sensor_id", "$sensor_id"), eq("recordDay", "$recordDay"));
        List<BsonField> groupOps = new ArrayList<>();
        groupOps.add(push("dataSamples", "$dataSamples"));
        groupOps.add(sum("nSample", 1L));
        groupOps.add(min("minTime", "$dataSamples.recordDate"));
        groupOps.add(max("maxTime", "$dataSamples.recordDate"));
        groupOps.add(min("minRSSI", "$dataSamples.RSSI"));
        groupOps.add(max("maxRSSI", "$dataSamples.RSSI"));
        groupOps.add(min("minLQI", "$dataSamples.LQI"));
        groupOps.add(max("maxLQI", "$dataSamples.LQI"));
        groupOps.add(min("minBattery", "$dataSamples.Battery"));
        groupOps.add(max("maxBattery", "$dataSamples.Battery"));
        Bson groupStage = group(groupID, groupOps);
        List<Field<?>> fields = new ArrayList<>();
        fields.add(new Field("temp1_measures", new Document("min", new Document("$min", "$dataSamples.temp1")).append("max", new Document("$max", "$dataSamples.temp1"))
                .append("average",
                        new Document("$avg", "$dataSamples.temp1"))
                .append("std",
                        new Document("$stdDevSamp", "$dataSamples.temp1"))
                .append("n-maxV",
                        new Document("$sum", "$dataSamples.temp1-maxV"))
                .append("n-minV",
                        new Document("$sum", "$dataSamples.temp1-minV"))));
        fields.add(new Field("temp2_measures",
                new Document("min",
                        new Document("$min", "$dataSamples.temp2"))
                        .append("max",
                                new Document("$max", "$dataSamples.temp2"))
                        .append("average",
                                new Document("$avg", "$dataSamples.temp2"))
                        .append("std",
                                new Document("$stdDevSamp", "$dataSamples.temp2"))
                        .append("n-maxV",
                                new Document("$sum", "$dataSamples.temp2-maxV"))
                        .append("n-minV",
                                new Document("$sum", "$dataSamples.temp2-minV"))));
        fields.add(new Field("temp3_measures",
                new Document("min",
                        new Document("$min", "$dataSamples.temp3"))
                        .append("max",
                                new Document("$max", "$dataSamples.temp3"))
                        .append("average",
                                new Document("$avg", "$dataSamples.temp3"))
                        .append("std",
                                new Document("$stdDevSamp", "$dataSamples.temp3"))
                        .append("n-maxV",
                                new Document("$sum", "$dataSamples.temp3-maxV"))
                        .append("n-minV",
                                new Document("$sum", "$dataSamples.temp3-minV"))));
        fields.add(new Field("temp4_measures",
                new Document("min",
                        new Document("$min", "$dataSamples.temp4"))
                        .append("max",
                                new Document("$max", "$dataSamples.temp4"))
                        .append("average",
                                new Document("$avg", "$dataSamples.temp4"))
                        .append("std",
                                new Document("$stdDevSamp", "$dataSamples.temp4"))
                        .append("n-maxV",
                                new Document("$sum", "$dataSamples.temp4-maxV"))
                        .append("n-minV",
                                new Document("$sum", "$dataSamples.temp4-minV"))));
        fields.add(new Field("temp5_measures",
                new Document("min",
                        new Document("$min", "$dataSamples.temp5"))
                        .append("max",
                                new Document("$max", "$dataSamples.temp5"))
                        .append("average",
                                new Document("$avg", "$dataSamples.temp5"))
                        .append("std",
                                new Document("$stdDevSamp", "$dataSamples.temp5"))
                        .append("n-maxV",
                                new Document("$sum", "$dataSamples.temp5-maxV"))
                        .append("n-minV",
                                new Document("$sum", "$dataSamples.temp5-minV"))));
        fields.add(new Field("loop1_measures",
                new Document("min",
                        new Document("$min", "$dataSamples.loop1"))
                        .append("max",
                                new Document("$max", "$dataSamples.loop1"))
                        .append("average",
                                new Document("$avg", "$dataSamples.loop1"))
                        .append("std",
                                new Document("$stdDevSamp", "$dataSamples.loop1"))
                        .append("n-maxV",
                                new Document("$sum", "$dataSamples.loop1-maxV"))
                        .append("n-minV",
                                new Document("$sum", "$dataSamples.loop1-minV"))));
        fields.add(new Field("loop2_measures",
                new Document("min",
                        new Document("$min", "$dataSamples.loop2"))
                        .append("max",
                                new Document("$max", "$dataSamples.loop2"))
                        .append("average",
                                new Document("$avg", "$dataSamples.loop2"))
                        .append("std",
                                new Document("$stdDevSamp", "$dataSamples.loop2"))
                        .append("n-maxV",
                                new Document("$sum", "$dataSamples.loop2-maxV"))
                        .append("n-minV",
                                new Document("$sum", "$dataSamples.loop2-minV"))));
        fields.add(new Field("pulse1_measures",
                new Document("min",
                        new Document("$min", "$dataSamples.pulse1"))
                        .append("max",
                                new Document("$max", "$dataSamples.pulse1"))
                        .append("average",
                                new Document("$avg", "$dataSamples.pulse1"))
                        .append("std",
                                new Document("$stdDevSamp", "$dataSamples.pulse1"))
                        .append("n-maxV",
                                new Document("$sum", "$dataSamples.pulse1-maxV"))
                        .append("n-minV",
                                new Document("$sum", "$dataSamples.pulse1-minV"))));
        fields.add(new Field("pulse2_measures",
                new Document("min",
                        new Document("$min", "$dataSamples.pulse2"))
                        .append("max",
                                new Document("$max", "$dataSamples.pulse2"))
                        .append("average",
                                new Document("$avg", "$dataSamples.pulse2"))
                        .append("std",
                                new Document("$stdDevSamp", "$dataSamples.pulse2"))
                        .append("n-maxV",
                                new Document("$sum", "$dataSamples.pulse2-maxV"))
                        .append("n-minV",
                                new Document("$sum", "$dataSamples.pulse2-minV"))));
        fields.add(new Field("analog1_measures",
                new Document("min",
                        new Document("$min", "$dataSamples.analog1"))
                        .append("max",
                                new Document("$max", "$dataSamples.analog1"))
                        .append("average",
                                new Document("$avg", "$dataSamples.analog1"))
                        .append("std",
                                new Document("$stdDevSamp", "$dataSamples.analog1"))
                        .append("n-maxV",
                                new Document("$sum", "$dataSamples.analog1-maxV"))
                        .append("n-minV",
                                new Document("$sum", "$dataSamples.analog1-minV"))));
        fields.add(new Field("analog2_measures",
                new Document("min",
                        new Document("$min", "$dataSamples.analog2"))
                        .append("max",
                                new Document("$max", "$dataSamples.analog2"))
                        .append("average",
                                new Document("$avg", "$dataSamples.analog2"))
                        .append("std",
                                new Document("$stdDevSamp", "$dataSamples.analog2"))
                        .append("n-maxV",
                                new Document("$sum", "$dataSamples.analog2-maxV"))
                        .append("n-minV",
                                new Document("$sum", "$dataSamples.analog2-minV"))));
        Bson fieldStage = addFields(fields);
        Bson projectStage = new Document("$project",
                new Document("_id", 0L)
                        .append("sensor_id", "$_id.sensor_id")
                        .append("recordDay", "$_id.recordDay")
                        .append("dataSamples", 1L)
                        .append("nSample", 1L)
                        .append("minTime", 1L)
                        .append("maxTime", 1L)
                        .append("minRSSI", 1L)
                        .append("maxRSSI", 1L)
                        .append("minLQI", 1L)
                        .append("maxLQI", 1L)
                        .append("minBattery", 1L)
                        .append("maxBattery", 1L)
                        .append("temp1",
                                new Document("$cond",
                                        new Document("if",
                                                new Document("$eq", Arrays.asList("$temp1_measures.average",
                                                        new BsonNull())))
                                                .append("then", "$$REMOVE")
                                                .append("else", "$temp1_measures")))
                        .append("temp2",
                                new Document("$cond",
                                        new Document("if",
                                                new Document("$eq", Arrays.asList("$temp2_measures.average",
                                                        new BsonNull())))
                                                .append("then", "$$REMOVE")
                                                .append("else", "$temp2_measures")))
                        .append("temp3",
                                new Document("$cond",
                                        new Document("if",
                                                new Document("$eq", Arrays.asList("$temp3_measures.average",
                                                        new BsonNull())))
                                                .append("then", "$$REMOVE")
                                                .append("else", "$temp3_measures")))
                        .append("temp4",
                                new Document("$cond",
                                        new Document("if",
                                                new Document("$eq", Arrays.asList("$temp4_measures.average",
                                                        new BsonNull())))
                                                .append("then", "$$REMOVE")
                                                .append("else", "$temp4_measures")))
                        .append("temp5",
                                new Document("$cond",
                                        new Document("if",
                                                new Document("$eq", Arrays.asList("$temp5_measures.average",
                                                        new BsonNull())))
                                                .append("then", "$$REMOVE")
                                                .append("else", "$temp5_measures")))
                        .append("loop1",
                                new Document("$cond",
                                        new Document("if",
                                                new Document("$eq", Arrays.asList("$loop1_measures.average",
                                                        new BsonNull())))
                                                .append("then", "$$REMOVE")
                                                .append("else", "$loop1_measures")))
                        .append("loop2",
                                new Document("$cond",
                                        new Document("if",
                                                new Document("$eq", Arrays.asList("$loop2_measures.average",
                                                        new BsonNull())))
                                                .append("then", "$$REMOVE")
                                                .append("else", "$loop2_measures")))
                        .append("pulse1",
                                new Document("$cond",
                                        new Document("if",
                                                new Document("$eq", Arrays.asList("$pulse1_measures.average",
                                                        new BsonNull())))
                                                .append("then", "$$REMOVE")
                                                .append("else", "$pulse1_measures")))
                        .append("pulse2",
                                new Document("$cond",
                                        new Document("if",
                                                new Document("$eq", Arrays.asList("$pulse2_measures.average",
                                                        new BsonNull())))
                                                .append("then", "$$REMOVE")
                                                .append("else", "$pulse2_measures")))
                        .append("analog1",
                                new Document("$cond",
                                        new Document("if",
                                                new Document("$eq", Arrays.asList("$analog1_measures.average",
                                                        new BsonNull())))
                                                .append("then", "$$REMOVE")
                                                .append("else", "$analog1_measures")))
                        .append("analog2",
                                new Document("$cond",
                                        new Document("if",
                                                new Document("$eq", Arrays.asList("$analog2_measures.average",
                                                        new BsonNull())))
                                                .append("then", "$$REMOVE")
                                                .append("else", "$analog2_measures"))));
        Bson mergeStage = eq("$merge", and(eq("into", "sensorDataTest"), eq("on", Arrays.asList("sensor_id", "recordDay")), eq("whenMatched", "merge")));
        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(matchStage);
        pipeline.add(groupStage);
        pipeline.add(fieldStage);
        pipeline.add(projectStage);
        pipeline.add(mergeStage);
        Object result = sensorData.aggregate(pipeline).allowDiskUse(true).first();
        System.out.println(result);
    }
}
