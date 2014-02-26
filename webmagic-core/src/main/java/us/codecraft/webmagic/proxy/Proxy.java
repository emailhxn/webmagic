package us.codecraft.webmagic.proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;

/**
 * ClassName:Proxy
 * 
 * @see
 * @Function: TODO ADD FUNCTION
 * @author ch
 * @version Ver 1.0
 * @Date 2014-2-16 上午11:28:56
 */
public class Proxy implements Delayed {
	public static final int ERROR_403 = 403;
	public static final int ERROR_404 = 404;
	public static final int ERROR_BANNED = 10000;
	public static final int ERROR_Proxy = 10001;
	public static final int SUCCESS = 200;

	private int reuseTimeInterval = 1500;// ms

	private final HttpHost httpHost;
	// private final long createTime = System.currentTimeMillis();

	private int failedNum = 0;
	private Long canReuseTime = 0L;
	private Long lastBorrowTime = System.currentTimeMillis();
	private Long responseTime = 0L;
	private int successNum = 0;
	private int borrowNum = 0;

	public int getSuccessNum() {
		return successNum;
	}

	public void successNumIncrement(int increment) {
		this.successNum += increment;
	}

	public Long getLastUseTime() {
		return lastBorrowTime;
	}

	public void setLastBorrowTime(Long lastBorrowTime) {
		this.lastBorrowTime = lastBorrowTime;
	}

	// public Long getResponseTime() {
	// return responseTime;
	// }

	public void recordResponse() {
		this.responseTime = (System.currentTimeMillis() - lastBorrowTime + responseTime) / 2;
		this.lastBorrowTime = System.currentTimeMillis();
	}

	private List<Integer> failedErrorType = new ArrayList<Integer>();

	// public Long getResponseTime() {
	// return System.currentTimeMillis() - lastUseTime;
	// }

	public List<Integer> getFailedErrorType() {
		return failedErrorType;
	}

	public void setFailedErrorType(List<Integer> failedErrorType) {
		this.failedErrorType = failedErrorType;
	}

	Proxy(HttpHost httpHost) {
		this.httpHost = httpHost;
		this.canReuseTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(reuseTimeInterval, TimeUnit.MILLISECONDS);
	}

	Proxy(HttpHost httpHost, int reuseInterval) {
		this.httpHost = httpHost;
		this.canReuseTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(reuseInterval, TimeUnit.MILLISECONDS);
	}

	public void fail(int failedErrorType) {
		this.failedNum++;
		this.failedErrorType.add(failedErrorType);
	}

	// public long getCreateTime() {
	// return createTime;
	// }
	//
	// public void setFailedNum(int failedNum) {
	// this.failedNum = failedNum;
	// }
	public void setFailedNum(int failedNum) {
		this.failedNum = failedNum;
	}

	public int getFailedNum() {
		return failedNum;
	}

	public String getFailedType() {
		String re = "";
		for (Integer i : this.failedErrorType) {
			re += i + " . ";
		}
		return re;
	}

	public HttpHost getHttpHost() {
		return httpHost;
	}

	public int getReuseTimeInterval() {
		return reuseTimeInterval;
	}

	public void setReuseTimeInterval(int reuseTimeInterval) {
		this.reuseTimeInterval = reuseTimeInterval;
		this.canReuseTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(reuseTimeInterval, TimeUnit.MILLISECONDS);

	}

	@Override
	public long getDelay(TimeUnit unit) {
		return unit.convert(canReuseTime - System.nanoTime(), unit.NANOSECONDS);
	}

	@Override
	public int compareTo(Delayed o) {
		Proxy that = (Proxy) o;
		return canReuseTime > that.canReuseTime ? 1 : (canReuseTime < that.canReuseTime ? -1 : 0);

	}

	@Override
	public String toString() {

		String re = "host:" + httpHost.getAddress().getHostAddress() + " >> " + responseTime + "ms" + " >> success:" + successNum*100.0/borrowNum + "% >> borrow:"
				+ borrowNum;
		return re;

	}

	public void borrowNumIncrement(int increment) {
		this.borrowNum += increment;
	}

	public int getBorrowNum() {
		return borrowNum;
	}
}
