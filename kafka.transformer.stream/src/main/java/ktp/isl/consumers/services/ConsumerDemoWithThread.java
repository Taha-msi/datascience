package ktp.isl.consumers.services;

import com.google.gson.Gson;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.*;
import ktp.isl.consumers.dao.MongoDao;
import ktp.isl.consumers.dao.SensorDataImpl;
import ktp.isl.consumers.models.DataPoint;
import ktp.isl.consumers.models.SensorBean;
import ktp.isl.consumers.models.SensorDataBean;
import ktp.isl.consumers.utils.Date;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static java.lang.Thread.sleep;

public class ConsumerDemoWithThread extends MongoDao {
    private Gson jsonParser = new Gson();
    private String topic = "sensor_data_test_4";
    private String bootstrapServer = "localhost:9092";
    private String groupId = "sensor_data_test_0";
    private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

    private ConsumerDemoWithThread() {
    }

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private void run() {
        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        // create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServer, groupId, topic, latch);
        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited!");
        }
        ));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted ", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());


        public ConsumerRunnable(String bootstrapServer,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;
            // create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            // earliest/latest/none
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
            //properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,"1000");// for big data processing default is 5 mitnutes

            // create consumer
            this.consumer = new KafkaConsumer<String, String>(properties);

            consumer.subscribe(Collections.singleton(topic));
        }

        private InsertOneModel writeToMongoDB(SensorBean meta, SensorDataBean data) {
            List<Document> dataSamples = new ArrayList<>();
            Date date = new Date();

            Document nested = new Document("gatewayID", data.getGatewayId())
                    .append("system_id", meta.getSystemId())
                    .append("recordDate", date.getDate(data.getRecordDate()))
                    .append("hour", date.getHour(data.getRecordDate()))
                    .append("Battery", data.getBattery())
                    .append("RSSI", data.getRSSI())
                    .append("LQI", data.getLQI());
            for (DataPoint dp : meta.getDataPoints()) {
                double point = 0;
                try {
                    point = SensorDataBean.class.getField(dp.getPoint()).getDouble(data);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                }
                nested.append(dp.getPoint(), point)
                        .append(dp.getPoint() + "-minT", dp.getMinT())
                        .append(dp.getPoint() + "-maxT", dp.getMaxT())
                        .append(dp.getPoint() + "-activeDate", dp.getThresholdActiveDate())
                        .append(dp.getPoint() + "-alarmDelay", dp.getAlarmDelay())
                        .append(dp.getPoint() + "-alarmActiveDate", dp.getStartDelay());
                if (point > dp.getMaxT())
                    nested.append(dp.getPoint() + "-maxV", 1);
                else
                    nested.append(dp.getPoint() + "-maxV", 0);
                if (point < dp.getMinT())
                    nested.append(dp.getPoint() + "-minV", 1);
                else
                    nested.append(dp.getPoint() + "-minV", 0);
            }
            dataSamples.add(nested);

            InsertOneModel insertOneModel = new InsertOneModel<>(
                    new Document("_id", new ObjectId())
                            .append("recordDay", date.getDay(data.getRecordDate()))
                            .append("sensor_id", meta.getSensorId())
                            .append("rowId", data.getRowId())
                            .append("dataSamples", dataSamples));
            return insertOneModel;
        }

        @Override
        public void run() {
            SensorDataImpl sensorDataImpl = new SensorDataImpl();
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    if (records.count() > 0) {
                        SensorDataBean dataBean;
                        List<Long> keyList = new ArrayList<>();
                        List<SensorDataBean> sensorDataBeans = new ArrayList<SensorDataBean>();
                        logger.info("Received " + records.count());
                        List<WriteModel<Document>> bulkWriter = new ArrayList<>();
                        for (ConsumerRecord<String, String> record : records) {
                            dataBean = jsonParser.fromJson(record.value(), SensorDataBean.class);
                            logger.info("sensor_id " + dataBean.getSensorId());
                            sensorDataBeans.add(dataBean);
                            keyList.add(Long.valueOf(record.key()));
                        }
                        Map<Long, SensorBean> meta = sensorDataImpl.getSensorMetaData(keyList);
                        sensorDataBeans.parallelStream().forEach(bean -> bulkWriter.add(writeToMongoDB(meta.get(bean.getSensorId()), bean)));
                        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
                        if (bulkWriter.isEmpty()) {
                            System.out.println("Nothing to insert!");
                        } else {
                            try {
                                BulkWriteResult bulkResult = sensorData.bulkWrite(bulkWriter, bulkWriteOptions);
                                // output the number of updated documents
                                System.out.println(MessageFormat.format("inserted {0} documents", bulkResult.getInsertedCount()));
                            } catch (MongoBulkWriteException e) {
                                logger.info("Primary key for several sensors is violated!!! ");
                            }
                            logger.info("Committing offsets... ");
                            consumer.commitSync();
                        }
                    } else {
                        logger.info("Received " + records.count());
                        System.out.println("Nothing to insert!");
                    }
                    try {
                        sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
