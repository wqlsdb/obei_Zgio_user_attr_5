
import com.alibaba.fastjson2.JSON;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class MyImpalaRichSource extends RichSourceFunction<UserBean> {
    private Connection conn = null;
    private PreparedStatement pstmt = null;
    private ResultSet result = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //step1.加载驱动
        Class.forName("com.cloudera.impala.jdbc41.Driver");
        //Class.forName("com.mysql.cj.jdbc.Driver");
        //获取连接
        conn = DriverManager.getConnection(
                "jdbc:impala://realtime-1:21050/zanalytics;AuthMech=0"
                , "admin"
                , "admin"
        );
        // 创建statement对象
        pstmt = conn.prepareStatement("SELECT zg_id,\n" +
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
                "FROM zanalytics.b_user_event_attr_5 where yw=202301");
    }

    @Override
    public void run(SourceContext<UserBean> ctx) throws Exception {
        result = pstmt.executeQuery();
        resultSetToList(result,ctx);
/*        //todo:5.调用自定义的方法，
        List<HashMap<String, Object>> mapUserBean = resultSetToList(result);
        //todo:6.遍历mapUserBean,把SQL结果
        for (Object mUserBean : mapUserBean) {
            ctx.collect(UserBean.toUserBean(JSON.toJSONString(mUserBean)));
        }*/

        TimeUnit.SECONDS.sleep(5);
    }
    //todo:定义一个方法，用于处理SQL结果集，/*思考：我们最终返回的是一个什么类型的数据，*/
    public static void resultSetToList(ResultSet rs,SourceContext<UserBean> ctx)throws java.sql.SQLException {
        if (rs == null)
            return;
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();//目的，得出表的总列数，便于后续循环的长度
        List<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
        while (rs.next()) {
            //todo:定义一个hashmap用于存放for循环遍历出来的每条key value对
            LinkedHashMap<String, Object> rowData = new LinkedHashMap<String, Object>(columnCount);
            //todo:3.通过for循环遍历出每一列的字段，直到最后一列
            for (int i = 1; i <= columnCount; i++) {
                if ("null".equals(rs.getObject(i))) {
                    //todo:遍历的结果需要存放到HashMap中，因此在外面定义个Map
                    rowData.put(metaData.getColumnName(i), null);
                } else {
                    rowData.put(metaData.getColumnName(i), rs.getObject(i));
                }
            }
            ctx.collect(UserBean.toUserBean(JSON.toJSONString(rowData)));

        }
    }
/*    public static List<HashMap<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
        //todo:1.首先需要获取表的结构信息，例如字段，字段名等
        if (rs == null)
            return Collections.emptyList();
            //return Collections.EMPTY_LIST;
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();//目的，得出表的总列数，便于后续循环的长度
        //todo:定义一个ArrayList<HashMap<String,Object>> list用于存放while循环遍历出的hashmap
        ArrayList<HashMap<String, Object>> list = new ArrayList<>();
        //todo:2.遍历SQL结果集
        while (rs.next()) {
            //todo:定义一个hashmap用于存放for循环遍历出来的每条key value对
            LinkedHashMap<String, Object> rowData = new LinkedHashMap<>(columnCount);
            //todo:3.通过for循环遍历出每一列的字段，直到最后一列
            for (int i = 1; i <= columnCount; i++) {
                if ("null".equals(rs.getObject(i))) {
                    //todo:遍历的结果需要存放到HashMap中，因此在外面定义个Map
                    rowData.put(metaData.getColumnName(i), null);
                } else {
                    rowData.put(metaData.getColumnName(i), rs.getObject(i));
                }
            }
            //todo:4.将while循环遍历出的每条数据存放在rowData中的字段及对应的值，放到ArrayList<HashMap<String,Object>> list
            list.add(rowData);

        }
        return list;

    }*/

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        if (null != result) {
            result.close();
        }
        if (null != pstmt) {
            pstmt.close();
        }
        if (null != conn) {
            conn.close();
        }
    }
}
