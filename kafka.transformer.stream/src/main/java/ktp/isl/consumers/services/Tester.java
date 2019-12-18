package ktp.isl.consumers.services;



import java.util.*;
import java.util.concurrent.*;


import ktp.isl.consumers.dao.MongoDao;
import ktp.isl.consumers.dao.SensorDataImpl;


public class Tester extends MongoDao {
    private String sens;
    private List<Integer> sensorsList;

    public Tester() {

    }

    public static void main(String[] args) {

            new Tester().run();

        }
        private void run()  {
            SensorDataImpl sensorData = new SensorDataImpl();
            sensorData.updateSensorData();
       }

}
