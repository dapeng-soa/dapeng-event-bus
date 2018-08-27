import com.today.eventbus.agent.support.parse.AgentConsumerXml;
import org.simpleframework.xml.core.Persister;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * desc: test
 *
 * @author hz.lei
 * @since 2018年05月26日 下午3:39
 */
public class TestExecutors {

    public static void main(String[] args) throws InterruptedException {
       /* String body = "dddddddd";
        String info = body.substring(0, 100);

        System.out.println(info);*/
        List<Persion> persions = new ArrayList<>();
        persions.add(new Persion("大佬", "22"));
        persions.add(new Persion("大佬1", "22"));


        String infoLog = persions.stream().map(event ->
                "\nPersion:[DB: " + event.name + ", Table: " + event.age + " .]")
                .collect(Collectors.joining(","));


        System.out.println(infoLog);
        /*ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.shutdown();
        executorService.awaitTermination(1,TimeUnit.HOURS);
        executorService.shutdown();*/
    }
}

class Persion {
    public String name;
    public String age;

    public Persion(String name, String age) {
        this.name = name;
        this.age = age;
    }
}
