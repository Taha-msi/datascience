package ktp.isl.consumers.models;

import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.Date;

public class DataPoint {
    @BsonProperty("lable")
    private String lable;
    @BsonProperty("point")
    private String point;
    @BsonProperty("unit")
    private String unit;
    @BsonProperty("maxT")
    private int maxT;
    @BsonProperty("minT")
    private int minT;
    @BsonProperty("start_delay")
    private Date startDelay;
    @BsonProperty("end_delay")
    private Date endDelay;
    @BsonProperty("threshold_active_date")
    private Date thresholdActiveDate;

    private int alarmDelay;

    public int getMaxT() {
        return maxT;
    }

    public void setMaxT(int maxT) {
        this.maxT = maxT;
    }

    public int getMinT() {
        return minT;
    }

    public void setMinT(int minT) {
        this.minT = minT;
    }

    public int getAlarmDelay() {
        return alarmDelay;
    }

    public void setAlarmDelay(int alarmDelay) {
        this.alarmDelay = alarmDelay;
    }

    public String getLable() {
        return lable;
    }

    public void setLable(String lable) {
        this.lable = lable;
    }

    public String getPoint() {
        return point;
    }

    public void setPoint(String point) {
        this.point = point;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }


    public Date getStartDelay() {
        return startDelay;
    }

    public void setStartDelay(Date startDelay) {
        this.startDelay = startDelay;
    }

    public Date getEndDelay() {
        return endDelay;
    }

    public void setEndDelay(Date endDelay) {
        this.endDelay = endDelay;
    }

    public Date getThresholdActiveDate() {
        return thresholdActiveDate;
    }

    public void setThresholdActiveDate(Date thresholdActiveDate) {
        this.thresholdActiveDate = thresholdActiveDate;
    }
}
