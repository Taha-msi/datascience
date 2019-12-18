package ktp.isl.consumers.models;

import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.*;

import java.util.List;
import java.util.Optional;

public class SensorCodec implements CollectibleCodec<SensorBean> {

    private final Codec<Document> documentCodec;

    public SensorCodec() {
        super();
        this.documentCodec = new DocumentCodec();
    }

    @Override
    public SensorBean generateIdIfAbsentFromDocument(SensorBean sensorBean) {
        return null;
    }

    @Override
    public boolean documentHasId(SensorBean sensorBean) {
        return false;
    }

    @Override
    public BsonValue getDocumentId(SensorBean sensorBean) {
        return null;
    }

    @Override
    public SensorBean decode(BsonReader bsonReader, DecoderContext decoderContext) {
        Document sensorDoc = documentCodec.decode(bsonReader, decoderContext);
        SensorBean sensor = new SensorBean();
        Optional<Long> optional = Optional.of(sensorDoc.getLong("sensor_id"));
        if (optional.isPresent()) {
            sensor.setSensorId(optional.get());
            sensor.setSystemId(sensorDoc.getString("system_id"));
            sensor.setDataPoints((List<Document>) sensorDoc.get("data_points"));
        }
        return sensor;
    }

    @Override
    public void encode(BsonWriter bsonWriter, SensorBean sensorBean, EncoderContext encoderContext) {
        /*Document sensorDoc = new Document();
        Double systemId = sensorBean.getSystemId();
        String systemName = sensorBean.getSystemName();
        List<DataPoint> dataPoints = sensorBean.getDataPoints();
        int sensorId = sensorBean.getSensorId();
        String sensorName = sensorBean.getSensorName();
        int activated = sensorBean.getActivated();
        List<String> dataChannels = sensorBean.getDataChannels();
      if (!systemId.isNaN())
            sensorDoc.put("system_id",systemId);
      if (null != systemName)
          sensorDoc.put("system_name",systemName);
      if (dataPoints.size()>0)
          sensorDoc.put("data_points",dataPoints);
      documentCodec.encode(bsonWriter, sensorDoc, encoderContext);*/
    }

    @Override
    public Class<SensorBean> getEncoderClass() {
        return SensorBean.class;
    }
}
