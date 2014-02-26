package us.codecraft.webmagic.pipeline;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;

/**
 * ClassName:MySQLPipeline
 * 
 * @see
 * @Function: TODO ADD FUNCTION
 * @author ch
 * @version Ver 1.0
 * @Date 2014-2-12 下午06:31:02
 */
public class MySQLPipeline implements Pipeline {

	private static final String url = "jdbc:mysql://192.168.19.198/wanfang?user=root&password=root&useUnicode=true&characterEncoding=utf-8";
	private static Connection conn;
	private static PreparedStatement insertSQL;
	private static Logger logger = LoggerFactory.getLogger(MySQLPipeline.class);
	private static Long startTime = System.currentTimeMillis();
	private static AtomicInteger count = new AtomicInteger(0);
	static {
		initConnection();
	}

	@Override
	public void process(ResultItems resultItems, Task task) {

		// 修改数据的代码
		if ((String) resultItems.get("title_cn") == null && ((String) resultItems.get("title_cn")).equals("")) {
			return;
		}
		try {
			if (conn == null || insertSQL == null) {
				initConnection();
			}
			insertSQL.setString(1, (String) resultItems.get("title_cn"));
			insertSQL.setString(2, nullIsOK((String) resultItems.get("title_en")));
			insertSQL.setString(3, nullIsOK((String) resultItems.get("abstract_cn"),7990));
			insertSQL.setString(4, nullIsOK((String) resultItems.get("abstract_en"),4990));
			insertSQL.setString(5, nullIsOK((String) resultItems.get("author_cn"),490));
			insertSQL.setString(6, nullIsOK((String) resultItems.get("author_en"),490));
			insertSQL.setString(7, nullIsOK((String) resultItems.get("workplace"),900));
			insertSQL.setString(8, nullIsOK((String) resultItems.get("journal_cn")));
			insertSQL.setString(9, nullIsOK((String) resultItems.get("journal_en")));
			insertSQL.setString(10, nullIsOK((String) resultItems.get("year")));
			insertSQL.setString(11, nullIsOK((String) resultItems.get("keyword_cn")));
			insertSQL.setString(12, nullIsOK((String) resultItems.get("keyword_en")));
			insertSQL.setString(13, resultItems.getRequest().getUrl());
			// System.out.println(insertSQL.get)
			insertSQL.executeUpdate();
			if (count.incrementAndGet() % 1000 == 0) {
				logger.info("time per thousand >>>> " + (System.currentTimeMillis() - startTime) / (count.get() * 1000.0) + "s");
			}
		} catch (SQLException e) {
			logger.error("mysql connection failed and retry", e);
			initConnection();
		}
	}

	private static void initConnection() {
		try {
			logger.info("init mysql connection");
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, "root", "root");
			String insql = "INSERT INTO wanfang_detail "
					+ "(title_cn,title_en,abstract_cn,abstract_en,author_cn,author_en,workplace,journal_cn,journal_en,year,keyword_cn,keyword_en,url)"
					+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
			insertSQL = conn.prepareStatement(insql);
			if (conn == null || insertSQL == null) {
				logger.info("init mysql connection error");
			}
		} catch (ClassNotFoundException e) {
			logger.error("mysql connection init error", e);

		} catch (SQLException e) {
			logger.error("mysql connection init error", e);

		}
	}

	public String nullIsOK(String s) {
		if (s == null) {
			return "";
		}
		return s;
	}

	public String nullIsOK(String s, int maxLen) {
		if (s == null) {
			return "";
		} else {
			if (s.length() > maxLen) {
				return s.substring(0, maxLen);
			}
		}
		return s;
	}
}
