package com.hyit.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hyit.entity.MessageEntity;
import com.hyit.parser.XMLParserConfiguration;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import static org.apache.flink.table.api.Expressions.$;


/**
 * @tips:
 * 此类展示了一个错误的列子，由于消费Kafka采用的是流式去执行，同时有没有设定水位线，
 * 导致消费的数据不会被提交，所以无法写入Hive，只有当中断程序的时候才会执行提交，
 * 可以在Hive中观察到数据
 */
public class KafkaSimpleConsumeToHiveStream {

    private static ConcurrentMap<String,String> confMap = null;

    private static Properties props = new Properties();

    private static String topic = "";


    static{
        XMLParserConfiguration.init();
        confMap = XMLParserConfiguration.map;
        confMap.forEach((key,value)->{
            if(!"topic".equalsIgnoreCase(key)){
                props.put(key,value);
            }else{
                topic = value;
            }
        });
    }

    public static void main(String[] args) throws Exception {

        KafkaSimpleConsumeToHiveStream simpleStreamClass = new KafkaSimpleConsumeToHiveStream();

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(executionEnvironment);

        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        executionEnvironment.enableCheckpointing(90000,CheckpointingMode.EXACTLY_ONCE);

        String name = "kafkaToHive";

        String database = "ods";

        String conf = "/usr/hdp/3.1.4.0-315/hive/conf";

        String version = "3.1.0";

        HiveCatalog hiveCatalog = new HiveCatalog(name,database,conf,version);

        DataStream<MessageEntity> kafkaStream = simpleStreamClass.findSourceStreamFromKafka(executionEnvironment);

        //kafkaStream.print();

        simpleStreamClass.transformStreamToTableAndPersist(streamTableEnv,
                kafkaStream,
                hiveCatalog,
                System.currentTimeMillis());

        executionEnvironment.execute("timeneat_stream_job_without_partition");

    }

    private DataStream<MessageEntity> findSourceStreamFromKafka(StreamExecutionEnvironment environment){
        Properties props1 = new Properties();
        props1.put("bootstrap.servers","172.30.84.183:6667,172.30.84.184:6667,172.30.84.185:6667");
        props1.put("group.id","test1");
        props1.put("auto.offset.reset","earliest");
        props1.put("partition.assignment.strategy","org.apache.kafka.clients.consumer.RangeAssignor");
        props1.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props1.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        return environment
                .addSource(new FlinkKafkaConsumer<String>("sk-test-mock",new SimpleStringSchema(),props1)).name("kafka_source")
                .map(new MapFunction<String, MessageEntity>() {
                    @Override
                    public MessageEntity map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        String measurePointId = jsonObject.getString("MEASUREPOINTID");
                        String measureTag = jsonObject.getString("MEASURETAG");
                        double value1 = jsonObject.getDouble("VALUE");
                        String sampleTime = jsonObject.getString("SAMPLETIME");
                        return new MessageEntity(measurePointId,measureTag,value1,sampleTime);
                    }
                }).name("source_stringformat_entity");
    }

    private void transformStreamToTableAndPersist(StreamTableEnvironment tableEnv, DataStream<MessageEntity> stream, HiveCatalog hiveCatalog,long timeMillis) throws ParseException{

        tableEnv.registerCatalog("kafkaToHive",hiveCatalog);

        tableEnv.useCatalog("kafkaToHive");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.useDatabase("ods");

        tableEnv.createTemporaryView("kafkaStreamTables",
                stream,
                $("measurePointId").as("measurepointid"),
                $("measureTag").as("measuretag"),
                $("value").as("value1"),
                $("sampleTime").as("sampletime"));

        //tableEnv.executeSql("DROP TABLE IF EXISTS ods_timeneat_sk_tjl");

//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS ods_timeneat_sk_tjl(measurepointid string," +
//                "measuretag string," +
//                "value1 decimal(30,8)," +
//                "sampletime string) partitioned by (daytime string) stored as parquet " +
//                "TBLPROPERTIES (\n" +
//                "  'partition.time-extractor.timestamp-pattern'='$daytime',\n" +
//                "  'sink.partition-commit.delay'='0s',\n" +
//                "  'sink.partition-commit.trigger'='partition-time',\n" +
//                "  'sink.partition-commit.policy.kind'='metastore'" +
//                ")");

//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS ods_timeneat_sk_tjl(measurepointid string," +
//                "measuretag string," +
//                "value1 decimal(30,8)," +
//                "sampletime string) partitioned by (daytime string) stored as parquet");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS ods_timeneat_sk_tjl_nopartition(measurepointid string," +
                "measuretag string," +
                "value1 decimal(30,8)," +
                "sampletime string) stored as parquet");

        //tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        tableEnv.executeSql("insert into ods.ods_timeneat_sk_tjl_nopartition select measurepointid,measuretag,value1,sampletime from kafkaStreamTables");
    }



}
