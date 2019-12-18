package ktp.isl.producers.services;


import ktp.isl.producers.dao.SensorDataDao;
import ktp.isl.producers.dao.SensorDataDaoImpl;
import ktp.isl.producers.models.SensorDataBean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Tester {

    public static void main(String[] args) throws ParseException {
        String date = "2019-11-25 14:20:17";
        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:m:s");
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd");
        System.out.println(date.split(" ")[1].split(":")[0]);
        System.out.println(format1.parse(date));
        System.out.println(format2.parse(date));


    }


}