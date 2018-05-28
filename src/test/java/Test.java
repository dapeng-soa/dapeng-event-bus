import com.today.eventbus.agent.support.parse.AgentConsumerXml;
import org.simpleframework.xml.core.Persister;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * desc: test
 *
 * @author hz.lei
 * @since 2018年05月26日 下午3:39
 */
public class Test {

    public static void main(String[] args) {
        Persister persister = new Persister();
        AgentConsumerXml config = null;
        File file;
        FileInputStream inputStream = null;
        try {
            //==images==//
            inputStream = new FileInputStream("conf/rest-consumer.xml");
            config = persister.read(AgentConsumerXml.class, inputStream);
        } catch (FileNotFoundException e) {
            try {
                //==develop==//
                file = ResourceUtils.getFile("classpath:agent.xml");
                config = persister.read(AgentConsumerXml.class, file);
                System.out.println(config);
            } catch (FileNotFoundException e1) {
                throw new RuntimeException("rest-consumer.xml in classpath and conf/ NotFound, please Settings");
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        } catch (Exception e) {
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                }
            }
        }
        System.out.println(config);
    }
}
