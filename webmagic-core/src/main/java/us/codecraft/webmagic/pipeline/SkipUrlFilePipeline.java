package us.codecraft.webmagic.pipeline;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.utils.FilePersistentBase;

/**
 * ClassName:SkipUrlFilePipeline
 * 
 * @see
 * @Function: TODO ADD FUNCTION
 * @author ch
 * @version Ver 1.0
 * @Date 2014-2-20 下午02:17:35
 */
public class SkipUrlFilePipeline extends FilePersistentBase implements Pipeline {

	private Logger logger = LoggerFactory.getLogger(getClass());

	public static final String FILEPATH = "data\\skipUrl.txt";
	private static AtomicInteger count = new AtomicInteger(0);
	private BufferedWriter bw;

	public SkipUrlFilePipeline() {
		this(FILEPATH);
	}

	public SkipUrlFilePipeline(String filePath) {
//		setPath("data");
		try {
			bw = new BufferedWriter(new FileWriter(getFile(FILEPATH), true));
		} catch (IOException e) {
			logger.error("SkipUrl file io error", e);
		}
	}

	@Override
	public void process(ResultItems resultItems, Task task) {
		if (bw == null) {
			try {
				bw = new BufferedWriter(new FileWriter(new File(FILEPATH), true));
			} catch (IOException e) {
				logger.error("SkipUrl file io error", e);
			}
		}

		try {
			bw.write(resultItems.getRequest().getUrl() + '\n');
			if (count.incrementAndGet() % 100 == 0) {
				bw.flush();
				logger.info("flush skip url to file");
			}
		} catch (IOException e) {
			logger.warn("write file error", e);
		}
	}
	public static void main(String[] args) {
		SkipUrlFilePipeline p=new SkipUrlFilePipeline();
	}

}