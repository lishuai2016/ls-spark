package project1.conf;



/**
 * 配置管理组件测试类
 * @author Administrator
 *
 */
public class ConfigurationManagerTest {

	public static void main(String[] args) {
		String testkey1 = ConfigurationManager.getProperty("jdbc.url");
		String testkey2 = ConfigurationManager.getProperty("spark.local");
		System.out.println(testkey1);
		System.out.println(testkey2);
	}

}
