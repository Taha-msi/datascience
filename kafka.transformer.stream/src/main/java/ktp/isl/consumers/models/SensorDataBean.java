package ktp.isl.consumers.models;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SensorDataBean {
    private String gatewayId;
    private Date recordDate;
    private Long sensorId;
    public int loop1;
    public int loop2;
    public float temp1;
    public float temp2;
    public float temp3;
    public float temp4;
    public float temp5;
    public float pulse1;
    public float pulse2;
    public float analog1;
    public float analog2;
    private float battery;
    private float rSSI;
    private float lQI;
    private int rowId;

    public int getRowId() {
        return rowId;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
    }

    public SensorDataBean() {
    }

    public String getGatewayId() {
        return gatewayId;
    }

    public void setGatewayId(String gatewayId) {
        this.gatewayId = gatewayId;
    }

    public String getRecordDate() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd H:mm:ss");
        return format.format(recordDate);
    }

    public void setRecordDate(String recordDate) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd H:mm:ss");
        try {
            this.recordDate = format.parse(recordDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public Long getSensorId() {
        return sensorId;
    }

    public void setSensorId(Long sensorId) {
        this.sensorId = sensorId;
    }

    public int getLoop1() {
        return loop1;
    }

    public void setLoop1(int loop1) {
        this.loop1 = loop1;
    }

    public int getLoop2() {
        return loop2;
    }

    public void setLoop2(int loop2) {
        this.loop2 = loop2;
    }

    public float getTemp1() {
        return temp1;
    }

    public void setTemp1(float temp1) {
        this.temp1 = temp1;
    }

    public float getTemp2() {
        return temp2;
    }

    public void setTemp2(float temp2) {
        this.temp2 = temp2;
    }

    public float getTemp3() {
        return temp3;
    }

    public void setTemp3(float temp3) {
        this.temp3 = temp3;
    }

    public float getTemp4() {
        return temp4;
    }

    public void setTemp4(float temp4) {
        this.temp4 = temp4;
    }

    public float getTemp5() {
        return temp5;
    }

    public void setTemp5(float temp5) {
        this.temp5 = temp5;
    }

    public float getPulse1() {
        return pulse1;
    }

    public void setPulse1(float pulse1) {
        this.pulse1 = pulse1;
    }

    public float getPulse2() {
        return pulse2;
    }

    public void setPulse2(float pulse2) {
        this.pulse2 = pulse2;
    }

    public float getAnalog1() {
        return analog1;
    }

    public void setAnalog1(float analog1) {
        this.analog1 = analog1;
    }

    public float getAnalog2() {
        return analog2;
    }

    public void setAnalog2(float analog2) {
        this.analog2 = analog2;
    }

    public float getBattery() {
        return battery;
    }

    public void setBattery(float battery) {
        this.battery = battery;
    }

    public float getRSSI() {
        return rSSI;
    }

    public void setRSSI(float rSSI) {
        this.rSSI = rSSI;
    }

    public float getLQI() {
        return lQI;
    }

    public void setLQI(float lQI) {
        this.lQI = lQI;
    }
}
