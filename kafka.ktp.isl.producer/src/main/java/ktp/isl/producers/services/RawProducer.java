package ktp.isl.producers.services;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ktp.isl.producers.dao.SensorDataDao;
import ktp.isl.producers.dao.SensorDataDaoImpl;
import ktp.isl.producers.models.SensorDataBean;
import ktp.isl.producers.utils.Date;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;

import static java.lang.Thread.*;


public class RawProducer {
    private String fetchTime;
    Logger logger = LoggerFactory.getLogger(RawProducer.class.getName());
    private SensorDataDao sensorDataDao;
    private String topic = "sensor_data";
    KafkaProducer<String, String> producer;
    private Gson gson;

    public RawProducer() {
        this.sensorDataDao = new SensorDataDaoImpl();
        this.producer = createKafkaProducer();
        GsonBuilder builder = new GsonBuilder();
        builder.setPrettyPrinting().serializeNulls();
        gson = builder.create();
    }

    public static void main(String[] args) {
        new RawProducer().run();
    }

    private void run() {
        // In the main project it is just a socket server listening to a port, taking a request, check security headers and
        // assign a thread to it from a live thread group thread job will decompress, decrypt, process, compress and put it
        // in kafka as a kafka producer, however, reading from database as a client is the current business.
        logger.info("Setup");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application ...");
            logger.info("shutting down client from twitter ...");
            logger.info("closing producer ...");
            producer.close();
            logger.info("Done!");
        }));
        List<Long> sensorIds = sensorDataDao.getSensorMeta(); // just for current business
        fetchTime = sensorDataDao.getLastTime(sensorIds); // just for current business
        while (true) {
            Optional<List<SensorDataBean>> optional = Optional.of(sensorDataDao.getAllSensors(fetchTime, sensorIds));
            List<SensorDataBean> sensorDataBeanList;
            if (optional.isPresent()){
                sensorDataBeanList = optional.get();
            } else{
                logger.info("No connection found!!!");
                try {
                    sleep(60000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
            if (sensorDataBeanList.size() > 0) {
                System.out.println(MessageFormat.format("Receiving {0} records", sensorDataBeanList.size()));
                fetchTime = getMaxDate(sensorDataBeanList);
                sensorDataBeanList.parallelStream().forEach(sensorDataBean ->
                        producer.send(new ProducerRecord<String, String>(topic, sensorDataBean.getSensorId() + "", gson.toJson(sensorDataBean)), new Callback() {
                            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                System.out.println("sensorId: " + sensorDataBean.getSensorId() + " recordDate: " + sensorDataBean.getRecordDate() + " rowId: " + sensorDataBean.getRowId());
                                if (e == null) {
                                    logger.info("Recieved new metadata. \n" +
                                            "Topic: " + recordMetadata.topic() + "\n" +
                                            "Partition: " + recordMetadata.partition() + "\n" +
                                            "Offset: " + recordMetadata.offset() + "\n" +
                                            "Timestamp: " + recordMetadata.timestamp());
                                } else {
                                    logger.error("Error while producing ", e);
                                }
                            }
                        })
                );
                producer.flush();
            } else
                System.out.println("Nothing to process");
            try {
                sleep(20000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sensorIds = sensorDataDao.getSensorMeta(); // just for current business
        }
    }

    private String getMaxDate(List<SensorDataBean> sensorDataBeanList) {
        Date date = new Date();
        String maxDate;
        Comparator<SensorDataBean> comparator = (p1, p2) -> date.getDate(p1.getRecordDate()).compareTo(date.getDate(p2.getRecordDate()));
        maxDate = sensorDataBeanList.stream().max(comparator).get().getRecordDate();
        return maxDate;
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "178.62.86.223:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "50");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        //properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "20"); // the time the .send() will block until throwing an exception
        //properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "20"); // The size of the send buffer which will fill up over time and fill back when the throughput to the broker increases.
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}

