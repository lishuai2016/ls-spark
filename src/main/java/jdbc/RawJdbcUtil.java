package jdbc;


import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;

import java.sql.*;
import java.util.List;
import java.util.Map;


public class RawJdbcUtil {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(RawJdbcUtil.class);
    // 定义数据库的链接
    private Connection conn;

    // 定义sql语句的执行对象
    private PreparedStatement pstmt;

    // 定义查询返回的结果集合
    private ResultSet rs;


    // 初始化   url中包括db数据库名称
    public RawJdbcUtil(String url, String username, String password) {
        try {
            //v1版本
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, username, password);
            log.info("数据库连接成功,对应的url:"+url);
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (null != conn) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            log.error("获取数据库链接出现异常");
        }
    }

    // 查询多条记录
    public List<Map<String, Object>> executeSql(String sql) throws SQLException {
        log.info("执行SQL："+sql);
        List<Map<String, Object>> resultList = Lists.newArrayList();
        pstmt = conn.prepareStatement(sql);
        rs = pstmt.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        int cols_len = metaData.getColumnCount();
        while (rs.next()) {
            Map<String, Object> map = Maps.newLinkedHashMap();
            for (int i = 0; i < cols_len; i ++) {//列字段个数
                String cols_name = metaData.getColumnLabel(i + 1);//列的名称
                Object cols_value = rs.getObject(cols_name);//列的值
                map.put(cols_name, cols_value != null ? cols_value : "");
            }
            resultList.add(map);//一行一个map添加
        }
        log.info("执行SQL：{}, 执行结果：{}",sql, resultList);
        return resultList;
    }

    // 释放连接
    public void release() {
        try {
            if (null != rs) {
                rs.close();
                rs=null;
            }
            if (null != pstmt) {
                pstmt.close();
                pstmt = null;
            }
            if (null != conn) {
                conn.close();
                conn = null;
            }

        } catch (SQLException e) {
            e.printStackTrace();
            log.error("释放数据库链接出现异常");
        }
        log.info("释放数据库连接");
    }


    public static void main(String[] args) {
        String url="jdbc:mysql://localhost:3306/test";
        String username="root";
        String pwd="123456";
        String sql = "select * from user limit 10";
        RawJdbcUtil rawJdbcUtil = null;
        try {
            rawJdbcUtil = new RawJdbcUtil(url,username,pwd);
            List<Map<String, Object>> mapList = rawJdbcUtil.executeSql(sql);
            System.out.println("结果："+ JSONObject.toJSONString(mapList));
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rawJdbcUtil != null) {
                rawJdbcUtil.release();
            }
        }
    }
}
