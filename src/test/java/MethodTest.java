import com.today.eventbus.common.ConsumerContext;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
 * @since 2018-11-27 12:21 PM
 */
public class MethodTest {

    public void send(ConsumerContext context, Object event) {


    }

    public static void main(String[] args) throws Exception {
        Method method = MethodTest.class.getMethod("send", ConsumerContext.class, Object.class);
        // send
        Parameter[] parameters = method.getParameters();

        for (int i = 0; i < parameters.length; i++) {
            boolean assignableFrom = ConsumerContext.class.isAssignableFrom(parameters[i].getType());

            System.out.println(assignableFrom);
            System.out.println(parameters[i]);
        }


    }
}
