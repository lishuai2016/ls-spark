package hbase.impl;


import hbase.HbaseDao;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by FromX on 2017/3/17.
 */

public class HbaseDaoImpl implements HbaseDao {

    // 声明静态配置
    private static Configuration conf = null;
    private static HBaseAdmin hAdmin = null;
    private static Connection conn = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.17.200.153,172.17.201.152,172.17.201.107");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "172.17.201.152" + ":60000");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        try {
            conn = ConnectionFactory.createConnection(conf);
            // 创建一个数据库管理员
            hAdmin = (HBaseAdmin) conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Override
    public void save(Put put, String tableName) throws IOException {
        // TODO Auto-generated method stub
        Table table = conn.getTable(TableName.valueOf(tableName));
        table.put(put);
        table.close();
    }

    @Override
    public void insert(String tableName, String rowKey, String family, String quailifer, String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(family.getBytes(), quailifer.getBytes(), value.getBytes());
        table.put(put);
        table.close();
    }

    @Override
    public void insert(String tableName, String rowKey, String family, String[] quailifer, String[] value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        // 批量添加
        for (int i = 0; i < quailifer.length; i++) {
            String col = quailifer[i];
            String val = value[i];
            put.addColumn(family.getBytes(), col.getBytes(), val.getBytes());
        }
        table.put(put);
        table.close();
    }

    @Override
    public void save(List<Put> Put, String tableName) throws IOException {
        // TODO Auto-generated method stub
        Table table = conn.getTable(TableName.valueOf(tableName));
        table.put(Put);
        table.close();
    }

    @Override
    public Result getOneRow(String tableName, String rowKey) throws IOException {
        // TODO Auto-generated method stub
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result rsResult = table.get(get);
        table.close();
        return rsResult;
    }

    @Override
    public List<Result> getRows(String tableName, String rowKey_like) throws IOException {
        // TODO Auto-generated method stub
        Table table = conn.getTable(TableName.valueOf(tableName));
        ;
        PrefixFilter filter = new PrefixFilter(rowKey_like.getBytes());
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        List<Result> list = new ArrayList<Result>();
        for (Result rs : scanner) {
            list.add(rs);
        }
        table.close();
        return list;
    }

    @Override
    public List<Result> getRows(String tableName, String rowKeyLike, String[] cols) throws IOException {
        // TODO Auto-generated method stub
        Table table = conn.getTable(TableName.valueOf(tableName));
        PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());

        Scan scan = new Scan();
        for (int i = 0; i < cols.length; i++) {
            scan.addColumn("cf".getBytes(), cols[i].getBytes());
        }
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        ArrayList<Result> list = new ArrayList<Result>();
        ;
        for (Result rs : scanner) {
            list.add(rs);
        }
        table.close();
        return list;
    }

    @Override
    public List<Result> getRows(String tableName, String startRow, String stopRow) throws IOException {

        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setStartRow(startRow.getBytes());
        scan.setStopRow(stopRow.getBytes());
        ResultScanner scanner = table.getScanner(scan);
        List<Result> list = new ArrayList<Result>();
        for (Result rsResult : scanner) {
            list.add(rsResult);
        }
        table.close();
        return list;

    }

    @Override
    public void deleteRecords(String tableName, String rowKeyLike) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        List<Delete> list = new ArrayList<Delete>();
        for (Result rs : scanner) {
            Delete del = new Delete(rs.getRow());
            list.add(del);
        }
        table.delete(list);
        table.close();
    }

    @Override
    public void deleteTable(String tableName) throws IOException {

        if (hAdmin.tableExists(tableName)) {
            hAdmin.disableTable(tableName);// 禁用表
            hAdmin.deleteTable(tableName);// 删除表
            System.err.println("删除表成功!");
        } else {
            System.err.println("删除的表不存在！");
        }
        //  hAdmin.close();
    }

    @Override
    public String createTable(String tableName, String[] columnFamilys) throws IOException {

        if (hAdmin.tableExists(tableName)) {
            return "table existed";
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(
                    TableName.valueOf(tableName));
            for (String columnFamily : columnFamilys) {
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            }
            hAdmin.createTable(tableDesc);
            return "success";
        }
        // hAdmin.close();// 关闭释放资源
    }

    @Override
    public List<Result> getRowsByOneKey(String tableName, String rowKeyLike, String[] cols) throws IOException {
        // TODO Auto-generated method stub
        Table table = conn.getTable(TableName.valueOf(tableName));
        PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());

        Scan scan = new Scan();
        for (int i = 0; i < cols.length; i++) {
            scan.addColumn("cf".getBytes(), cols[i].getBytes());
        }
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        List<Result> list = new ArrayList<Result>();
        for (Result rs : scanner) {
            list.add(rs);
        }
        table.close();
        return list;
    }

    @Override
    public Result getOneRowAndMultiColumn(String tableName, String rowKey, String[] cols) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        for (int i = 0; i < cols.length; i++) {
            get.addColumn("cf".getBytes(), cols[i].getBytes());
        }
        Result rsResult = table.get(get);
        table.close();
        return rsResult;
    }
}
