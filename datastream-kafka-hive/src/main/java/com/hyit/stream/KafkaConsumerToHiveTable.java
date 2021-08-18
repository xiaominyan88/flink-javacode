package com.hyit.stream;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.Arrays;

public class KafkaConsumerToHiveTable {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        executionEnvironment.enableCheckpointing(90000, CheckpointingMode.EXACTLY_ONCE);

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(executionEnvironment,environmentSettings);

        String name = "kafkaToHive";

        String database = "ods";

        String conf = "/usr/hdp/3.1.4.0-315/hive/conf";

        String version = "3.1.0";

        HiveCatalog hiveCatalog = new HiveCatalog(name,database,conf,version);

        tableEnv.registerCatalog("kafkaToHive",hiveCatalog);

        //Flink在建立主键的时候只支持NOT ENFORCED模式
        //format为json的时候不支持主键约束
        tableEnv.executeSql("CREATE TABLE kafka_source (\n" +
                "MEASUREPOINTID STRING,\n" +
                "MEASURETAG STRING,\n" +
                "`VALUE` DOUBLE,\n" +
                "SAMPLETIME STRING,\n" +
                "TS AS to_timestamp(SAMPLETIME),\n" +
//                "CONSTRAINT source_pk PRIMARY KEY (MEASUREPOINTID,MEASURETAG,SAMPLETIME) NOT ENFORCED,\n" +
                "WATERMARK FOR TS AS TS - INTERVAL '3' MINUTE\n" +
                ") WITH (\n" +
                "   'connector' = 'kafka',\n" +
                "   'topic' = 'sk-test-mock',\n" +
                "   'properties.bootstrap.servers' = '172.30.84.183:6667,172.30.84.184:6667,172.30.84.185:6667',\n" +
                "   'properties.group.id' = 'test_new',\n" +
                "   'scan.startup.mode' = 'group-offsets',\n" +
                "   'format' = 'json'\n" +
                ")");

        System.out.println("开始打印");

        Arrays.stream(tableEnv.listTables()).forEach(m->System.out.println("表名称：" + m));

        tableEnv.executeSql("SELECT * FROM kafka_source LIMIT 2").print();

        System.out.println("=======打印结束=======");


//        tableEnv.executeSql("SET table.sql-dialect=hive");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS kafkaToHive.ods.ods_sk_tjl_15min_parition (\n" +
                "MEAUSREPOINTID STRING,\n" +
                "MEASURETAG STRING,\n" +
                "VALUE1 decimal(30,8),\n" +
                "SAMPLETIME STRING\n" +
                ") PARTITIONED BY (dt STRING, hr STRING) STORED AS PARQUET TBLPROPERTIES(\n" +
                "  'sink.partition-commit.trigger' = 'partition-time',\n" +
                "  'partition.time-extractor.timestamp-pattern' = '$dt $hr:00:00',\n" +
                "  'sink.partition-commit.delay' = '15 min',\n" +
                "  'sink.partition-commit.policy.kind' = 'metastore,success-file'\n" +
                ")");

        tableEnv.executeSql("INSERT INTO TABLE kafkaToHive.ods.ods_sk_tjl_15min_parition " +
                "SELECT MEASUREPOINTID,MEASURETAG,VALUE1,SAMPLETIME,DATE_FORMAT(TS,'yyyy-MM-dd'),DATE_FORMAT(TS,'HH') FROM kafka_source");

        tableEnv.execute("kafka_source_to_hive_sql");

    }
}
