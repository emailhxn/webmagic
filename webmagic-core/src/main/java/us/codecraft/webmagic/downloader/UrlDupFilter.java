package us.codecraft.webmagic.downloader;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.codecraft.webmagic.pipeline.MySQLPipeline;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * ClassName:BloomFilter
 * 
 * @see
 * @Function: TODO ADD FUNCTION
 * @author ch
 * @version Ver 1.0
 * @Date 2014-2-16 下午10:20:05
 */
public class UrlDupFilter {
	/**
	 * <p>
	 * Copyright ® 2002 国家科技基础平台技术信息中心版权所有。
	 * </p>
	 */
	private BloomFilter<CharSequence> filter;
	private static Logger logger = LoggerFactory.getLogger(UrlDupFilter.class);

	public UrlDupFilter() {
		this.filter = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")), 20000000, 0.0000001F);
		initFilter();
	}

	public synchronized void put(String url) {
		if (!filter.put(url)) {
			// logger.debug("put false>>>" + url);
		}
	}

	// public

	public boolean contains(String url) {
		return filter.mightContain(url);
	}

	public void clean() {
		this.filter = null;
	}

	public void initFilter() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://192.168.19.198/wanfang?user=root&password=root&useUnicode=true&characterEncoding=utf-8";
			Connection conn = DriverManager.getConnection(url, "root", "root");
			logger.info("initFilter mysql connection！");
			//select url from wanfang_detail where id >= (select id from wanfang_detail limit offset,1) limit length
			int length = 1000000;
			int count = 0;
			String sql1 = "select url from wanfang_detail where id>= (select id from wanfang_detail limit ";
			String sql2=",1) limit "+length;
			Statement stmt = conn.createStatement();
			for (int i = 0;; i += length) {
				Long time = System.currentTimeMillis();
				ResultSet rs = stmt.executeQuery(sql1 + i + sql2);
				logger.info("read database >>>> " + (System.currentTimeMillis()-time)/1000.0+"s");
				if (!rs.last()) {
					break;
				} else {
					rs.beforeFirst();
				}
				while (rs.next()) {
					// if(!filter.mightContain(url)){
					filter.put(rs.getString(1));
					// }else{
					// System.out.println(url);
					// }
					count++;
				}
				rs.close();
			}
			logger.info("filter size:" + count);
			logger.info("filter size:" + count);
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		UrlDupFilter filter = new UrlDupFilter();
//		filter.initFilter();
	}
}
