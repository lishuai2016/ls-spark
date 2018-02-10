package hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;

/**
 * Created by FromX on 2017/3/17.
 */
public interface HbaseDao {
    void save(Put put, String tableName) throws IOException;

    void insert(String tableName, String rowKey, String family, String quailifer, String value) throws IOException;

    void insert(String tableName, String rowKey, String family, String quailifer[], String value[]) throws IOException;

    void save(List<Put> Put, String tableName) throws IOException;

    Result getOneRow(String tableName, String rowKey) throws IOException;

    List<Result> getRows(String tableName, String rowKey_like) throws IOException;

    List<Result> getRows(String tableName, String rowKeyLike, String cols[]) throws IOException;

    List<Result> getRows(String tableName, String startRow, String stopRow) throws IOException;

    void deleteRecords(String tableName, String rowKeyLike) throws IOException;

    void deleteTable(String tableName) throws IOException;

    String createTable(String tableName, String[] columnFamilys) throws IOException;

    List<Result> getRowsByOneKey(String tableName, String rowKeyLike, String[] cols) throws IOException;

    Result getOneRowAndMultiColumn(String tableName, String rowKey, String[] cols) throws IOException;
}
