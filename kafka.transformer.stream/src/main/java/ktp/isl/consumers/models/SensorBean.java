package ktp.isl.consumers.models;

import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SensorBean {

    @BsonProperty("system_id")
    private String systemId;
    @BsonProperty("data_points")
    private List<DataPoint> dataPoints;
    @BsonProperty("sensor_id")
    private Long sensorId;

    public String getSystemId() {
        return systemId;
    }

    public void setSystemId(String systemId) {
        this.systemId = systemId;
    }

    public List<DataPoint> getDataPoints() {
        return dataPoints;
    }

    public void setDataPoints(List<Document> dataPointsDoc) {
        this.dataPoints = new ArrayList<DataPoint>();
        final DataPoint[] point = new DataPoint[1];
        dataPointsDoc.forEach(doc -> {
            point[0] = new DataPoint();
            point[0].setLable(doc.getString("lable"));
            point[0].setPoint(doc.getString("point"));
            point[0].setMaxT( doc.getInteger("maxT"));
            point[0].setMinT(doc.getInteger("minT"));
            point[0].setEndDelay(doc.getDate("end_delay"));
            point[0].setStartDelay(doc.getDate("start_delay"));
            point[0].setThresholdActiveDate(doc.getDate("threshold_active_date"));
            point[0].setUnit(doc.getString("unit"));
            Date date = new Date(System.currentTimeMillis());
            if (date.compareTo(point[0].getEndDelay()) < 0 && date.compareTo(point[0].getStartDelay()) > 0)
                point[0].setAlarmDelay(1);
            else
                point[0].setAlarmDelay(0);
            this.dataPoints.add(point[0]);
            point[0] = null;
        });
    }

    public Long getSensorId() {
        return sensorId;
    }

    public void setSensorId(long sensorId) {
        this.sensorId = sensorId;
    }


}
