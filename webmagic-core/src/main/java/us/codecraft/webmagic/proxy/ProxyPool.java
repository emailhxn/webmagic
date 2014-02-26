package us.codecraft.webmagic.proxy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClassName:ProxyPool
 * 
 * @see
 * @Function: TODO ADD FUNCTION
 * @author ch
 * @version Ver 1.0
 * @Date 2014-2-14 下午01:10:04
 */
public class ProxyPool {
	private BlockingQueue<Proxy> proxyQueue = new DelayQueue<Proxy>();
	private Logger logger = LoggerFactory.getLogger(getClass());
	private int reuseInterval = 1500;// ms
	private int reviveTime = 2 * 60 * 60 * 1000;// ms
	private Map<String, Proxy> allProxy = new ConcurrentHashMap<String, Proxy>();

	public ProxyPool(List<String[]> httpProxyList) {
		for (String[] s : httpProxyList) {
			try {
				HttpHost item = new HttpHost(InetAddress.getByName(s[0]), Integer.valueOf(s[1]));
				// if (ProxyUtil.validateProxy(item)) {
				Proxy p = new Proxy(item, reuseInterval);
				proxyQueue.add(p);
				allProxy.put(s[0], p);
				// }
			} catch (NumberFormatException e) {
				logger.error("HttpHost init error:", e);
			} catch (UnknownHostException e) {
				logger.error("HttpHost init error:", e);
			}
		}

		logger.info("proxy pool size>>>>" + this.proxyQueue.size());
	}

	public ProxyPool() {

	}

	public void addProxy(String[]... httpProxyList) {
		for (String[] s : httpProxyList) {
			try {
				HttpHost item = new HttpHost(InetAddress.getByName(s[0]), Integer.valueOf(s[1]));
				// if (ProxyUtil.validateProxy(item)) {
				Proxy p = new Proxy(item, reuseInterval);
				proxyQueue.add(p);
				allProxy.put(s[0], p);
				// }
			} catch (NumberFormatException e) {
				logger.error("HttpHost init error:", e);
			} catch (UnknownHostException e) {
				logger.error("HttpHost init error:", e);
			}
		}
	}

	public HttpHost getProxy() {
		Proxy proxy = null;
		try {

			Long time = System.currentTimeMillis();
			proxy = proxyQueue.take();
			double costTime = (System.currentTimeMillis() - time) / 1000.0;
			if (costTime > reuseInterval) {
				logger.info("get proxy time >>>> " + costTime);
			}
			Proxy p=allProxy.get(proxy.getHttpHost().getAddress().getHostAddress());
			p.setLastBorrowTime(System.currentTimeMillis());
			p.borrowNumIncrement(1);
		} catch (InterruptedException e) {
			logger.error("get proxy error",e);
		}
		if (proxy == null) {
			throw new NoSuchElementException();
		}
		return proxy.getHttpHost();
	}

	public void returnProxy(HttpHost host, int statusCode) {
		Proxy p = allProxy.get(host.getAddress().getHostAddress());
		if (p == null) {
			return;
		}
		switch (statusCode) {
		case Proxy.SUCCESS:
			p.setReuseTimeInterval(reuseInterval);
			p.setFailedNum(0);
			p.setFailedErrorType(new ArrayList<Integer>());
			p.recordResponse();
			p.successNumIncrement(1);
			break;
		case Proxy.ERROR_403:
			// banned,try larger interval
			p.fail(Proxy.ERROR_403);
			p.setReuseTimeInterval(reuseInterval * p.getFailedNum());
			logger.info(host + " >>>> reuseTimeInterval is >>>> " + p.getReuseTimeInterval() / 1000.0);
			break;
		case Proxy.ERROR_BANNED:
			p.fail(Proxy.ERROR_BANNED);
			p.setReuseTimeInterval(10 * 60 * 1000 * p.getFailedNum());
			logger.warn("this proxy is banned >>>> " + p.getHttpHost());
			logger.info(host + " >>>> reuseTimeInterval is >>>> " + p.getReuseTimeInterval() / 1000.0);
			break;
		case Proxy.ERROR_404:
			p.fail(Proxy.ERROR_404);
			// p.setReuseTimeInterval(reuseInterval * p.getFailedNum());
			break;
		default:
			p.fail(statusCode);
			break;
		}
		if (p.getFailedNum() > 20) {
			allProxy.remove(host.getAddress().getHostAddress());
			p.setReuseTimeInterval(reviveTime);
			logger.error("remove proxy >>>> " + host + ">>>>" + p.getFailedType() + " >>>> remain proxy >>>> " + allProxy.size());
			logger.info(allProxyStutus());
			return;
		}
		if (p.getFailedNum() == 10) {
			if (!ProxyUtil.validateProxy(host)) {
				allProxy.remove(host.getAddress().getHostAddress());
				p.setReuseTimeInterval(reviveTime);
				logger.error("remove proxy >>>> " + host + ">>>>" + p.getFailedType() + " >>>> remain proxy >>>> " + allProxy.size());
				logger.info(allProxyStutus());
				return;
			}
		}
		try {
			proxyQueue.put(p);
		} catch (InterruptedException e) {
			logger.warn("proxyQueue return proxy error", e);
		}
	}

	public String allProxyStutus(){
		String re="all proxy info >>>> \n";
		for(Entry<String, Proxy> entry : allProxy.entrySet()) {
		   re+=entry.getValue().toString()+"\n";
		}
		return re;
		
	}

	public int getIdleNum() {
		return proxyQueue.size();
	}

	public int getReuseInterval() {
		return reuseInterval;
	}

	public void setReuseInterval(int reuseInterval) {
		this.reuseInterval = reuseInterval;
	}

	public void shouldWait(HttpHost host) {
		Proxy p = allProxy.get(host.getAddress().getHostAddress());
	}

	public static List<String[]> getProxyList() {
		List<String[]> proxyList = new ArrayList<String[]>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(new File("proxy.txt")));

			String line = "";
			while ((line = br.readLine()) != null) {
				proxyList.add(new String[] { line.split(":")[0], line.split(":")[1] });
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return proxyList;
	}

	public static void main(String[] args) throws IOException {
		ProxyPool proxyPool = new ProxyPool(getProxyList().subList(0, 5));
		proxyPool.setReuseInterval(10000);
		while (true) {
			List<HttpHost> httphostList = new ArrayList<HttpHost>();
			System.in.read();
			int i = 0;
			while (proxyPool.getIdleNum() > 2) {
				HttpHost httphost = proxyPool.getProxy();
				httphostList.add(httphost);
				// proxyPool.proxyPool.use(httphost);
				proxyPool.logger.info("borrow object>>>>" + i + ">>>>" + httphostList.get(i).toString());
				i++;
			}
			System.out.println(proxyPool.allProxyStutus());
			System.in.read();
			for (i = 0; i < httphostList.size(); i++) {
				proxyPool.returnProxy(httphostList.get(i), 200);
				proxyPool.logger.info("return object>>>>" + i + ">>>>" + httphostList.get(i).toString());
			}
			System.out.println(proxyPool.allProxyStutus());
			System.in.read();
			
		}
	}
}
