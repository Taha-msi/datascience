package ktp.isl.consumers.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Date {
    private SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:m:s");
    private SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd");

    public java.util.Date getDay(String strDate) {
        java.util.Date date = new java.util.Date();
        try {
            date = format2.parse(strDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    public java.util.Date getCurrentDate(){
        java.util.Date date = new java.util.Date();
        String dateStr = format1.format(date);
        try {
            date =  format1.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    public java.util.Date getCurrentDay(){
        java.util.Date date = new java.util.Date();
        String dateStr = format2.format(date);
        try {
            date =  format2.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    public java.util.Date getDate(String strDate) {
        java.util.Date date = new java.util.Date();
        try {
            date = format1.parse(strDate);
        } catch (ParseException e) {

            e.printStackTrace();

        }
        return date;
    }

    public int getHour(String strDate) {
        return Integer.valueOf(strDate.split(" ")[1].split(":")[0]);

    }


}
