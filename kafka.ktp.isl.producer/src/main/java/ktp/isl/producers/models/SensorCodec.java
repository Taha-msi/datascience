package ktp.isl.producers.models;

import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.*;

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
        if(optional.isPresent())
            sensor.setSensorId(optional.get());
        return sensor;
    }

    @Override
    public void encode(BsonWriter bsonWriter, SensorBean sensorBean, EncoderContext encoderContext) {
        }

    @Override
    public Class<SensorBean> getEncoderClass() {
        return SensorBean.class;
    }
}
