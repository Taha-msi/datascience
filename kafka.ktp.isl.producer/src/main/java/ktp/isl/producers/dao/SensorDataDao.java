package ktp.isl.producers.dao;

import ktp.isl.producers.models.SensorDataBean;

import java.util.List;

public interface SensorDataDao {
    public List<SensorDataBean> getAllSensors(String fetchTime,List<Long> sensorIds);
    public String getLastTime(List<Long> sensorIds);
    public List<Long> getSensorMeta();



}
