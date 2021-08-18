package com.hyit.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hyit.entity.MessageEntity;
import com.hyit.parser.XMLParserConfiguration;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import static org.apache.flink.table.api.Expressions.$;

public class KafkaConsumeToHiveStream {

    private static ConcurrentMap<String,String> confMap = null;

    private static Properties props = new Properties();
    
    private static String topic = "";

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static long time_interval = 15*60*1000L;

    private static String timeRegex = "^((([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8]))))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))-02-29))\\s+([0-1]?[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$";

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

        KafkaConsumeToHiveStream streamClass = new KafkaConsumeToHiveStream();

//        Configuration configuration = new Configuration();
//
//        configuration.setString("classloader.resolve-order","child-first");
//
//        configuration.setString("jobmanager.rpc.address","172.30.84.180");
//
//        configuration.setString("jobmanager.rpc.port","6123");
//
//        configuration.setString("jobmanager.memory.process.size","1600m");
//
//        configuration.setString("taskmanager.memory.process.size","1728m");
//
//        configuration.setInteger("taskmanager.numberOfTaskSlots",1);
//
//        configuration.setInteger("parallelism.default",1);
//
//        configuration.setString("jobmanager.execution.failover-strategy","region");

        long timeMillis = System.currentTimeMillis();

//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

//        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(executionEnvironment,settings);

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(executionEnvironment);

        String name = "kafkaToHive";

        String database = "ods";

        String conf = "/usr/hdp/3.1.4.0-315/hive/conf";

        String version = "3.1.0";

        HiveCatalog hiveCatalog = new HiveCatalog(name,database,conf,version);

        DataStream<MessageEntity> kafkaSourceStream = streamClass.findSourceStreamFromKafka(executionEnvironment);

        streamClass.transformStreamToTableAndPersist(streamTableEnv,
                streamClass.keyedAndReducedStream(kafkaSourceStream),
                hiveCatalog,
                timeMillis);

        executionEnvironment.execute("timeneat_stream_job_test");

    }

    private DataStream<MessageEntity> findSourceStreamFromKafka(StreamExecutionEnvironment environment){
        return environment
                .addSource(new FlinkKafkaConsumer<String>("sk-test-mock",new SimpleStringSchema(),props))
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
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<MessageEntity>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                        .withTimestampAssigner(
                        new SerializableTimestampAssigner<MessageEntity>() {
                            @Override
                            public long extractTimestamp(MessageEntity element, long recordTimestamp) {
                                long extractTime = 0L;

                                try{
                                    if(element.getSampleTime() != null && !"".equals(element.getSampleTime())){
                                        extractTime = Pattern.matches(timeRegex,element.getSampleTime())?sdf.parse(element.getSampleTime()).getTime():57600000L;
                                    }else{
                                        extractTime = 57600000L;
                                    }
                                }catch (ParseException ex){
                                    ex.printStackTrace();
                                }
                                return extractTime;
                            }
                        }
                ));
    }

    private WindowedStream<MessageEntity, Tuple2<String, String>, TimeWindow> findwindowStream(DataStream<MessageEntity> sourceStream){
        return sourceStream.keyBy(new KeyPartitionEntity()).window(TumblingEventTimeWindows.of(Time.minutes(15)));
    }

    private DataStream<MessageEntity> keyedAndReducedStream(DataStream<MessageEntity> sourceStream){
        return sourceStream.keyBy(new KeyPartitionEntity())
                .window(TumblingEventTimeWindows.of(Time.minutes(15)))
                .reduce(new TimeReduceFunction(),new TimeNeatProcessWindowFunction());
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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS ods_timeneat_sk_tjl(measurepointid string," +
                "measuretag string," +
                "value1 decimal(30,8)," +
                "sampletime string) partitioned by (daytime string) stored as parquet " +
                "TBLPROPERTIES (\n" +
                "  'partition.time-extractor.timestamp-pattern'='$daytime',\n" +
                "  'sink.partition-commit.delay'='0s',\n" +
                "  'sink.partition-commit.trigger'='partition-time',\n" +
                "  'sink.partition-commit.policy.kind'='metastore'");

        //tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        tableEnv.executeSql("insert into ods.ods_timeneat_sk_tjl select measurepointid,measuretag,value1,sampletime,from_unixtime(" + timeMillis + ",'yyyy-MM-dd') from kafkaStreamTables");
    }

    class TimeReduceFunction implements ReduceFunction<MessageEntity> {

        @Override
        public MessageEntity reduce(MessageEntity value1, MessageEntity value2) throws Exception {

            long time1 = 57600000L;

            long time2 = 57600000L;

            if(value1.getSampleTime() != null && !"".equals(value1.getSampleTime())){

                time1 = sdf.parse(value1.getSampleTime()).getTime();

            }

            if(value2.getSampleTime() != null && !"".equals(value2.getSampleTime())){

                time2 = sdf.parse(value2.getSampleTime()).getTime();

            }

            if(time1 > time2){

                return value1;

            }else if(time1 < time2){

                return value2;

            }else{

                return value1;
            }

        }
    }


    class TimeNeatProcessWindowFunction extends ProcessWindowFunction<MessageEntity, MessageEntity, Tuple2<String, String>, TimeWindow> {

        @Override
        public void process(Tuple2<String, String> key, Context context, Iterable<MessageEntity> elements, Collector<MessageEntity> out) throws Exception {

            long start = context.window().getStart();

            long end = context.window().getEnd();

            Iterator<MessageEntity> elementIterator = elements.iterator();

            while(elementIterator.hasNext()){

                MessageEntity message = elementIterator.next();

                long sampleTimeMillis = 0L;

                if(message.getSampleTime() != null && !"".equals(message.getSampleTime())){
                    sampleTimeMillis = Pattern.matches(timeRegex,message.getSampleTime())?sdf.parse(message.getSampleTime()).getTime():57600000L;
                }else{
                    sampleTimeMillis = 57600000L;
                }

                if(sampleTimeMillis >= start && sampleTimeMillis <= end){

                    if((sampleTimeMillis - start) <= time_interval/2) {
                        message.setSampleTime(sdf.format(new Date(start)));
                    }else {
                        message.setSampleTime(sdf.format(new Date(end)));
                    }

                }

                out.collect(message);

            }

        }
    }


    /*
     *此类所对应的{@link assignTimestampsAndWatermarks()}已经作废
     */
    @Deprecated
    class RecordTimeStampAndWaterMarkAssigner extends BoundedOutOfOrdernessTimestampExtractor<MessageEntity>{

        public RecordTimeStampAndWaterMarkAssigner(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(MessageEntity element) {

            long extractTime = 0L;

            try{
                extractTime = sdf.parse(element.getSampleTime()).getTime();
            }catch (ParseException ex){
                ex.printStackTrace();
            }
            return extractTime;
        }
    }

    class KeyPartitionEntity implements KeySelector<MessageEntity, Tuple2<String,String>>{

        private static final long serialVersionUID = 4780234853172462378L;

        @Override
        public Tuple2<String, String> getKey(MessageEntity value) throws Exception {
            return new Tuple2<String,String>(value.getMeasurePointId(),value.getMeasureTag());
        }
    }
}
