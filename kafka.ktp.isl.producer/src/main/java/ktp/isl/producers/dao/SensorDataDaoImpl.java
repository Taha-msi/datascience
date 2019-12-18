package ktp.isl.producers.dao;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import ktp.isl.producers.models.SensorBean;
import ktp.isl.producers.models.SensorCodec;
import ktp.isl.producers.models.SensorDataBean;
import org.bson.Document;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Supplier;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.excludeId;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

public class SensorDataDaoImpl extends MongoDao implements SensorDataDao {

    @Override
    public List<SensorDataBean> getAllSensors(String maxTime, List<Long> sensorIds) {
        List<SensorDataBean> sensorDataBeanList = new ArrayList<>();
        try {
            Connection conn = getConnection(); // just for current business
            StringBuilder stringBuilder = new StringBuilder("SELECT A.gateway_id as gatewayID, A.record_time as recordDate, " +
                    "A.sensor_id as sensorID,0 as loop1, 0 as loop2, B.temperature as temp1, 0 as temp2, 0 as temp3,0 as temp4,0 as temp5,0 as pulse1,0 as pulse2, 0 as analog1,0 as analog2," +
                    " A.battery as Battery, A.rssi as RSSI, A.snr as LQI, B.diagnostics_row_id as row_id from dataset.datatype_diagnostics as A, dataset.datatype_107 as B  " +
                    "where B.diagnostics_row_id=A.row_id and B.sensor_id in(");
            sensorIds.forEach(sens -> stringBuilder.append(sens + ", "));
            stringBuilder.append("0) and A.record_time > ? order by A.sensor_id, A.record_time");
            PreparedStatement statement = conn.prepareStatement(stringBuilder.toString());
            statement.setString(1, maxTime);
            ResultSet rs = statement.executeQuery();
            SensorDataBean sensorDataBean;
            while (rs.next()) {
                sensorDataBean = parseLine(rs);
                sensorDataBeanList.add(sensorDataBean);
                sensorDataBean = null;
            }
            String query = "SELECT A.gateway_id as gatewayID, A.record_time as recordDate, A.sensor_id as sensorID,0 as loop1, 0 as loop2,  B.reading_temp_cur as temp1, B.reading_temp_min as temp2, B.reading_temp_max as temp3 ,0 as temp4,0 as  temp5,0 as pulse1,0 as pulse2,0 as analog1,0 as analog2, A.battery as Battery, A.rssi as RSSI, A.snr as LQI, B.diagnostics_row_id as row_id"
                    + " from dataset.datatype_diagnostics as A, dataset.datatype_1 as B "
                    + " where B.diagnostics_row_id=A.row_id and "
                    + " B.sensor_id =6312937 and A.record_time > ? order by A.sensor_id, A.record_time";
            PreparedStatement statement2 = conn.prepareStatement(query);
            statement2.setString(1, maxTime);
            ResultSet rs2 = statement2.executeQuery();
            while (rs2.next()) {
                sensorDataBean = parseLine(rs2);
                sensorDataBeanList.add(sensorDataBean);
            }
            statement.close();
            removeConnection(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sensorDataBeanList;
    }

    public String getLastTime(List<Long> sensorIds) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd H:mm:ss");
        Bson sensMatch = in("sensor_id", sensorIds);
        Bson matchStage = Aggregates.match(sensMatch);
        Long groupId = 0L;
        BsonField max1 = Accumulators.max("maxTime", "$maxTime");
        Bson groupStage = Aggregates.group(groupId, max1);
        Bson project = excludeId();
        Bson projectStage = Aggregates.project(project);
        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(matchStage);
        pipeline.add(groupStage);
        pipeline.add(projectStage);
        Document maxTime = null;
        maxTime = new Document("maxTime", "2019-11-10T00:00:00");
        Optional<Object> optional = Optional.of(sensorData.aggregate(pipeline).first());
        if (optional.isPresent())
            maxTime = (Document) optional.get();
        return format.format(maxTime.get("maxTime"));
    }

    public List<Long> getSensorMeta() {
        SensorCodec sensorCodec = new SensorCodec();
        List<Long> sensorList = new ArrayList<>();
        CodecRegistry codecRegistry =
                fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), fromCodecs(sensorCodec));
        Bson queryFilter =
                and(
                        eq("activated", 1)
                        //,in("sensor_id", sensorsList)
                );
        MongoCollection<SensorBean> sensors = db.getCollection("devices", SensorBean.class).withCodecRegistry(codecRegistry);
        List<SensorBean> sensorBeans = new ArrayList<>();
        sensors.find(queryFilter).into(sensorBeans);
        sensorBeans.forEach(res -> sensorList.add(res.getSensorId()));
        return sensorList;
    }

    private SensorDataBean parseLine(ResultSet rs) {
        Supplier<SensorDataBean> beanSupplier = SensorDataBean::new;
        SensorDataBean sensorDataBean = beanSupplier.get();
        try {
            sensorDataBean.setGatewayId(rs.getString(1));
            sensorDataBean.setRecordDate(rs.getString(2));
            sensorDataBean.setSensorId(rs.getLong(3));
            sensorDataBean.setLoop1(rs.getInt(4));
            sensorDataBean.setLoop2(rs.getInt(5));
            sensorDataBean.setTemp1(rs.getFloat(6));
            sensorDataBean.setTemp2(rs.getFloat(7));
            sensorDataBean.setTemp3(rs.getFloat(8));
            sensorDataBean.setTemp4(rs.getFloat(9));
            sensorDataBean.setTemp5(rs.getFloat(10));
            sensorDataBean.setPulse1(rs.getFloat(11));
            sensorDataBean.setPulse2(rs.getFloat(12));
            sensorDataBean.setAnalog1(rs.getFloat(13));
            sensorDataBean.setAnalog2(rs.getFloat(14));
            sensorDataBean.setBattery(rs.getFloat(15));
            sensorDataBean.setRSSI(rs.getFloat(16));
            sensorDataBean.setLQI(rs.getFloat(17));
            sensorDataBean.setRowId(rs.getInt(18));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sensorDataBean;
    }
}
