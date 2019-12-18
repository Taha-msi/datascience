package ktp.isl.producers.models;


import org.bson.codecs.pojo.annotations.BsonProperty;


public class SensorBean {

    @BsonProperty("sensor_id")
    private Long sensorId;

    public Long getSensorId() {
        return sensorId;
    }

    public void setSensorId(Long sensorId) {
        this.sensorId = sensorId;
    }


}
