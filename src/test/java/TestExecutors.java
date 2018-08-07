import com.today.eventbus.agent.support.parse.AgentConsumerXml;
import org.simpleframework.xml.core.Persister;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * desc: test
 *
 * @author hz.lei
 * @since 2018年05月26日 下午3:39
 */
public class TestExecutors {

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.shutdown();
        executorService.awaitTermination(1,TimeUnit.HOURS);
        executorService.shutdown();
    }
}
