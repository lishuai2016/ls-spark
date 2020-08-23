package ls.spark.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

public class ConnectionPool {

	// 静态连接队列
	private static LinkedList<Connection> connectionQueue;

	static{
		// 加载驱动
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	// 获取连接多线程访问并发控制
	public synchronized static Connection getConnection(){

		try {
			if(connectionQueue==null){
				connectionQueue = new LinkedList<Connection>();
				for(int i=0; i<10; i++){
					Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/testdb",
							"root","123456");
					connectionQueue.push(conn);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return connectionQueue.poll();
	}

	public static void returnConnection(Connection conn){
		connectionQueue.push(conn);
	}
}
