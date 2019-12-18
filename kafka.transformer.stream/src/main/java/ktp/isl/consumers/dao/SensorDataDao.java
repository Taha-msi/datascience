package ktp.isl.consumers.dao;

import ktp.isl.consumers.models.SensorBean;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface SensorDataDao {
    public Map<Long, SensorBean> getSensorMetaData(List<Long> sensorsList);

    public void updateSensorData(List<Long> sensorsList, Date currentDay);

    void updateSensorData(List<Long> sensorsList);

    public void updateSensorData();
}
