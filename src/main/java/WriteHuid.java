import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class WriteHuid {
    public static void extracted(StreamExecutionEnvironment env, DataStreamSource<UserBean> userBeanDataStreamSource){
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView("view_b_user_event_attr_5_2023",userBeanDataStreamSource);
        tableEnv.executeSql("create table b_user_event_attr_5_2023 (\n" +
                "`ZG_ID` int,\n" +
                "`SESSION_ID` varchar,\n" +
                "`UUID` varchar,\n" +
                "`EVENT_ID` varchar,\n" +
                "`BEGIN_DATE` varchar,\n" +
                "`begin_time_id` varchar,\n" +
                "`DEVICE_ID` varchar,\n" +
                "`USER_ID` varchar,\n" +
                "`EVENT_NAME` varchar,\n" +
                "`begin_day_id` varchar,\n" +
                "`PLATFORM` varchar,\n" +
                "`network` varchar,\n" +
                "`mccmnc` varchar,\n" +
                "`USERAGENT` varchar,\n" +
                "`WEBSITE` varchar,\n" +
                "`CURRENT_URL` varchar,\n" +
                "`REFERRER_URL` varchar,\n" +
                "`CHANNEL` varchar,\n" +
                "`APP_VERSION` varchar,\n" +
                "`IP` varchar,\n" +
                "`ip_str` varchar,\n" +
                "`COUNTRY` varchar,\n" +
                "`AREA` varchar,\n" +
                "`CITY` varchar,\n" +
                "`OS` varchar,\n" +
                "`OV` varchar,\n" +
                "`BS` varchar,\n" +
                "`BV` varchar,\n" +
                "`UTM_SOURCE` varchar,\n" +
                "`UTM_MEDIUM` varchar,\n" +
                "`UTM_CAMPAIGN` varchar,\n" +
                "`UTM_CONTENT` varchar,\n" +
                "`UTM_TERM` varchar,\n" +
                "`DURATION` varchar,\n" +
                "`utc_date` varchar,\n" +
                "`ATTR1` varchar,\n" +
                "`ATTR2` varchar,\n" +
                "`ATTR3` varchar,\n" +
                "`ATTR4` varchar,\n" +
                "`ATTR5` varchar,\n" +
                "`cus1` varchar,\n" +
                "`cus2` varchar,\n" +
                "`cus3` varchar,\n" +
                "`cus4` varchar,\n" +
                "`cus5` varchar,\n" +
                "`cus6` varchar,\n" +
                "`cus7` varchar,\n" +
                "`cus8` varchar,\n" +
                "`cus9` varchar,\n" +
                "`cus10` varchar,\n" +
                "`cus11` varchar,\n" +
                "`cus12` varchar,\n" +
                "`cus13` varchar,\n" +
                "`cus14` varchar,\n" +
                "`cus15` varchar,\n" +
                "`cus16` varchar,\n" +
                "`cus17` varchar,\n" +
                "`cus18` varchar,\n" +
                "`cus19` varchar,\n" +
                "`cus20` varchar,\n" +
                "`cus21` varchar,\n" +
                "`cus22` varchar,\n" +
                "`cus23` varchar,\n" +
                "`cus24` varchar,\n" +
                "`cus25` varchar,\n" +
                "`cus26` varchar,\n" +
                "`cus27` varchar,\n" +
                "`cus28` varchar,\n" +
                "`cus29` varchar,\n" +
                "`cus30` varchar,\n" +
                "`cus31` varchar,\n" +
                "`cus32` varchar,\n" +
                "`cus33` varchar,\n" +
                "`cus34` varchar,\n" +
                "`cus35` varchar,\n" +
                "`cus36` varchar,\n" +
                "`cus37` varchar,\n" +
                "`cus38` varchar,\n" +
                "`cus39` varchar,\n" +
                "`cus40` varchar,\n" +
                "`cus41` varchar,\n" +
                "`cus42` varchar,\n" +
                "`cus43` varchar,\n" +
                "`cus44` varchar,\n" +
                "`cus45` varchar,\n" +
                "`cus46` varchar,\n" +
                "`cus47` varchar,\n" +
                "`cus48` varchar,\n" +
                "`cus49` varchar,\n" +
                "`cus50` varchar,\n" +
                "`cus51` varchar,\n" +
                "`cus52` varchar,\n" +
                "`cus53` varchar,\n" +
                "`cus54` varchar,\n" +
                "`cus55` varchar,\n" +
                "`cus56` varchar,\n" +
                "`cus57` varchar,\n" +
                "`cus58` varchar,\n" +
                "`cus59` varchar,\n" +
                "`cus60` varchar,\n" +
                "`cus61` varchar,\n" +
                "`cus62` varchar,\n" +
                "`cus63` varchar,\n" +
                "`cus64` varchar,\n" +
                "`cus65` varchar,\n" +
                "`cus66` varchar,\n" +
                "`cus67` varchar,\n" +
                "`cus68` varchar,\n" +
                "`cus69` varchar,\n" +
                "`cus70` varchar,\n" +
                "`cus71` varchar,\n" +
                "`cus72` varchar,\n" +
                "`cus73` varchar,\n" +
                "`cus74` varchar,\n" +
                "`cus75` varchar,\n" +
                "`cus76` varchar,\n" +
                "`cus77` varchar,\n" +
                "`cus78` varchar,\n" +
                "`cus79` varchar,\n" +
                "`cus80` varchar,\n" +
                "`cus81` varchar,\n" +
                "`cus82` varchar,\n" +
                "`cus83` varchar,\n" +
                "`cus84` varchar,\n" +
                "`cus85` varchar,\n" +
                "`cus86` varchar,\n" +
                "`cus87` varchar,\n" +
                "`cus88` varchar,\n" +
                "`cus89` varchar,\n" +
                "`cus90` varchar,\n" +
                "`cus91` varchar,\n" +
                "`cus92` varchar,\n" +
                "`cus93` varchar,\n" +
                "`cus94` varchar,\n" +
                "`cus95` varchar,\n" +
                "`cus96` varchar,\n" +
                "`cus97` varchar,\n" +
                "`cus98` varchar,\n" +
                "`cus99` varchar,\n" +
                "`cus100` varchar,\n" +
                "`type1` varchar,\n" +
                "`type2` varchar,\n" +
                "`type3` varchar,\n" +
                "`type4` varchar,\n" +
                "`type5` varchar,\n" +
                "`type6` varchar,\n" +
                "`type7` varchar,\n" +
                "`type8` varchar,\n" +
                "`type9` varchar,\n" +
                "`type10` varchar,\n" +
                "`type11` varchar,\n" +
                "`type12` varchar,\n" +
                "`type13` varchar,\n" +
                "`type14` varchar,\n" +
                "`type15` varchar,\n" +
                "`type16` varchar,\n" +
                "`type17` varchar,\n" +
                "`type18` varchar,\n" +
                "`type19` varchar,\n" +
                "`type20` varchar,\n" +
                "`type21` varchar,\n" +
                "`type22` varchar,\n" +
                "`type23` varchar,\n" +
                "`type24` varchar,\n" +
                "`type25` varchar,\n" +
                "`type26` varchar,\n" +
                "`type27` varchar,\n" +
                "`type28` varchar,\n" +
                "`type29` varchar,\n" +
                "`type30` varchar,\n" +
                "`type31` varchar,\n" +
                "`type32` varchar,\n" +
                "`type33` varchar,\n" +
                "`type34` varchar,\n" +
                "`type35` varchar,\n" +
                "`type36` varchar,\n" +
                "`type37` varchar,\n" +
                "`type38` varchar,\n" +
                "`type39` varchar,\n" +
                "`type40` varchar,\n" +
                "`type41` varchar,\n" +
                "`type42` varchar,\n" +
                "`type43` varchar,\n" +
                "`type44` varchar,\n" +
                "`type45` varchar,\n" +
                "`type46` varchar,\n" +
                "`type47` varchar,\n" +
                "`type48` varchar,\n" +
                "`type49` varchar,\n" +
                "`type50` varchar,\n" +
                "`type51` varchar,\n" +
                "`type52` varchar,\n" +
                "`type53` varchar,\n" +
                "`type54` varchar,\n" +
                "`type55` varchar,\n" +
                "`type56` varchar,\n" +
                "`type57` varchar,\n" +
                "`type58` varchar,\n" +
                "`type59` varchar,\n" +
                "`type60` varchar,\n" +
                "`type61` varchar,\n" +
                "`type62` varchar,\n" +
                "`type63` varchar,\n" +
                "`type64` varchar,\n" +
                "`type65` varchar,\n" +
                "`type66` varchar,\n" +
                "`type67` varchar,\n" +
                "`type68` varchar,\n" +
                "`type69` varchar,\n" +
                "`type70` varchar,\n" +
                "`type71` varchar,\n" +
                "`type72` varchar,\n" +
                "`type73` varchar,\n" +
                "`type74` varchar,\n" +
                "`type75` varchar,\n" +
                "`type76` varchar,\n" +
                "`type77` varchar,\n" +
                "`type78` varchar,\n" +
                "`type79` varchar,\n" +
                "`type80` varchar,\n" +
                "`type81` varchar,\n" +
                "`type82` varchar,\n" +
                "`type83` varchar,\n" +
                "`type84` varchar,\n" +
                "`type85` varchar,\n" +
                "`type86` varchar,\n" +
                "`type87` varchar,\n" +
                "`type88` varchar,\n" +
                "`type89` varchar,\n" +
                "`type90` varchar,\n" +
                "`type91` varchar,\n" +
                "`type92` varchar,\n" +
                "`type93` varchar,\n" +
                "`type94` varchar,\n" +
                "`type95` varchar,\n" +
                "`type96` varchar,\n" +
                "`type97` varchar,\n" +
                "`type98` varchar,\n" +
                "`type99` varchar,\n" +
                "`type100` varchar,\n" +
                "`eid` varchar,\n" +
                "`yw` int,\n" +
                "primary key(`ZG_ID`,`SESSION_ID`,`UUID`,`EVENT_ID`,`BEGIN_DATE`,`begin_time_id`) not enforced\n" +
                ")\n" +
                "PARTITIONED BY (`yw`)\n" +
                "with(\n" +
                "'connector'='hudi',\n" +
                "'path'= 'hdfs://mycluster/hudi_Zgio.db/b_user_event_attr_5_2023Test',\n" +
                "'table.type'='COPY_ON_WRITE',\n" +
                "'write.operation'='bulk_insert',\n" +
                "'spillable_map_base_path'='hdfs://mycluster/flinktmp',\n" +
                "'write.tasks' = '50',--对小文件个数没有影响\n" +
                "--'index.type'='BUCKET',\n" +
                "--'hoodie.bucket.index.hash.field'='ZG_ID,UUID',\n" +
                "--'hoodie.bucket.index.num.buckets'='10',\n" +
                "--'write.bucket_assign.tasks' = '10',\n" +
                "'write.rate.limit'='500000',\n" +
                "'hoodie.cleaner.policy' = 'KEEP_LATEST_COMMITS',\n" +
                "'clean.retain_commits' = '1',\n" +
                "'hive_sync.db' = 'user_behavior',\n" +
                "'hive_sync.enable' = 'true',\n" +
                "'hive_sync.file_format' = 'PARQUET',\n" +
                "'hive_sync.mode' = 'hms',\n" +
                "'hive_sync.metastore.uris' = 'thrift://PVVMON0296:9083,thrift://PVVMON0308:9083,thrift://PVVMON0295:9083',\n" +
                "'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor',\n" +
                "'hive_sync.partition_fields' = 'yw',\n" +
                "'hive_sync.table' = 'b_user_event_attr_5_2023_hive',\n" +
                "'hive_sync.support_timestamp' = 'true'\n" +
                ")");
        tableEnv.sqlQuery("SELECT zg_id,\n" +
                        "session_id,\n" +
                        "uuid,\n" +
                        "event_id,\n" +
                        "begin_date,\n" +
                        "begin_time_id,\n" +
                        "device_id,\n" +
                        "user_id,\n" +
                        "event_name,\n" +
                        "begin_day_id,\n" +
                        "platform,\n" +
                        "network,\n" +
                        "mccmnc,\n" +
                        "useragent,\n" +
                        "website,\n" +
                        "current_url,\n" +
                        "referrer_url,\n" +
                        "channel,\n" +
                        "app_version,\n" +
                        "ip,\n" +
                        "ip_str,\n" +
                        "country,\n" +
                        "area,\n" +
                        "city,\n" +
                        "os,\n" +
                        "ov,\n" +
                        "bs,\n" +
                        "bv,\n" +
                        "utm_source,\n" +
                        "utm_medium,\n" +
                        "utm_campaign,\n" +
                        "utm_content,\n" +
                        "utm_term,\n" +
                        "duration,\n" +
                        "utc_date,\n" +
                        "attr1,\n" +
                        "attr2,\n" +
                        "attr3,\n" +
                        "attr4,\n" +
                        "attr5,\n" +
                        "cus1,\n" +
                        "cus2,\n" +
                        "cus3,\n" +
                        "cus4,\n" +
                        "cus5,\n" +
                        "cus6,\n" +
                        "cus7,\n" +
                        "cus8,\n" +
                        "cus9,\n" +
                        "cus10,\n" +
                        "cus11,\n" +
                        "cus12,\n" +
                        "cus13,\n" +
                        "cus14,\n" +
                        "cus15,\n" +
                        "cus16,\n" +
                        "cus17,\n" +
                        "cus18,\n" +
                        "cus19,\n" +
                        "cus20,\n" +
                        "cus21,\n" +
                        "cus22,\n" +
                        "cus23,\n" +
                        "cus24,\n" +
                        "cus25,\n" +
                        "cus26,\n" +
                        "cus27,\n" +
                        "cus28,\n" +
                        "cus29,\n" +
                        "cus30,\n" +
                        "cus31,\n" +
                        "cus32,\n" +
                        "cus33,\n" +
                        "cus34,\n" +
                        "cus35,\n" +
                        "cus36,\n" +
                        "cus37,\n" +
                        "cus38,\n" +
                        "cus39,\n" +
                        "cus40,\n" +
                        "cus41,\n" +
                        "cus42,\n" +
                        "cus43,\n" +
                        "cus44,\n" +
                        "cus45,\n" +
                        "cus46,\n" +
                        "cus47,\n" +
                        "cus48,\n" +
                        "cus49,\n" +
                        "cus50,\n" +
                        "cus51,\n" +
                        "cus52,\n" +
                        "cus53,\n" +
                        "cus54,\n" +
                        "cus55,\n" +
                        "cus56,\n" +
                        "cus57,\n" +
                        "cus58,\n" +
                        "cus59,\n" +
                        "cus60,\n" +
                        "cus61,\n" +
                        "cus62,\n" +
                        "cus63,\n" +
                        "cus64,\n" +
                        "cus65,\n" +
                        "cus66,\n" +
                        "cus67,\n" +
                        "cus68,\n" +
                        "cus69,\n" +
                        "cus70,\n" +
                        "cus71,\n" +
                        "cus72,\n" +
                        "cus73,\n" +
                        "cus74,\n" +
                        "cus75,\n" +
                        "cus76,\n" +
                        "cus77,\n" +
                        "cus78,\n" +
                        "cus79,\n" +
                        "cus80,\n" +
                        "cus81,\n" +
                        "cus82,\n" +
                        "cus83,\n" +
                        "cus84,\n" +
                        "cus85,\n" +
                        "cus86,\n" +
                        "cus87,\n" +
                        "cus88,\n" +
                        "cus89,\n" +
                        "cus90,\n" +
                        "cus91,\n" +
                        "cus92,\n" +
                        "cus93,\n" +
                        "cus94,\n" +
                        "cus95,\n" +
                        "cus96,\n" +
                        "cus97,\n" +
                        "cus98,\n" +
                        "cus99,\n" +
                        "cus100,\n" +
                        "type1,\n" +
                        "type2,\n" +
                        "type3,\n" +
                        "type4,\n" +
                        "type5,\n" +
                        "type6,\n" +
                        "type7,\n" +
                        "type8,\n" +
                        "type9,\n" +
                        "type10,\n" +
                        "type11,\n" +
                        "type12,\n" +
                        "type13,\n" +
                        "type14,\n" +
                        "type15,\n" +
                        "type16,\n" +
                        "type17,\n" +
                        "type18,\n" +
                        "type19,\n" +
                        "type20,\n" +
                        "type21,\n" +
                        "type22,\n" +
                        "type23,\n" +
                        "type24,\n" +
                        "type25,\n" +
                        "type26,\n" +
                        "type27,\n" +
                        "type28,\n" +
                        "type29,\n" +
                        "type30,\n" +
                        "type31,\n" +
                        "type32,\n" +
                        "type33,\n" +
                        "type34,\n" +
                        "type35,\n" +
                        "type36,\n" +
                        "type37,\n" +
                        "type38,\n" +
                        "type39,\n" +
                        "type40,\n" +
                        "type41,\n" +
                        "type42,\n" +
                        "type43,\n" +
                        "type44,\n" +
                        "type45,\n" +
                        "type46,\n" +
                        "type47,\n" +
                        "type48,\n" +
                        "type49,\n" +
                        "type50,\n" +
                        "type51,\n" +
                        "type52,\n" +
                        "type53,\n" +
                        "type54,\n" +
                        "type55,\n" +
                        "type56,\n" +
                        "type57,\n" +
                        "type58,\n" +
                        "type59,\n" +
                        "type60,\n" +
                        "type61,\n" +
                        "type62,\n" +
                        "type63,\n" +
                        "type64,\n" +
                        "type65,\n" +
                        "type66,\n" +
                        "type67,\n" +
                        "type68,\n" +
                        "type69,\n" +
                        "type70,\n" +
                        "type71,\n" +
                        "type72,\n" +
                        "type73,\n" +
                        "type74,\n" +
                        "type75,\n" +
                        "type76,\n" +
                        "type77,\n" +
                        "type78,\n" +
                        "type79,\n" +
                        "type80,\n" +
                        "type81,\n" +
                        "type82,\n" +
                        "type83,\n" +
                        "type84,\n" +
                        "type85,\n" +
                        "type86,\n" +
                        "type87,\n" +
                        "type88,\n" +
                        "type89,\n" +
                        "type90,\n" +
                        "type91,\n" +
                        "type92,\n" +
                        "type93,\n" +
                        "type94,\n" +
                        "type95,\n" +
                        "type96,\n" +
                        "type97,\n" +
                        "type98,\n" +
                        "type99,\n" +
                        "type100,\n" +
                        "eid,\n" +
                        "yw\n" +
                        "FROM view_b_user_event_attr_5_2023")
                .executeInsert("b_user_event_attr_5_2023");
    }
}
