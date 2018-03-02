
/**
 * 描述:
 *
 * @author hz.lei
 * @date 2018年03月02日 下午5:41
 */
public class Test {

    int count = 1;

    public void test() {
        synchronized (this) {
            while (count <= 100) {
                System.out.println(Thread.currentThread().getName() + " : " + count);
                count++;
                this.notify();
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }


    public static void main(String[] args) {
        Test test = new Test();


        new Thread(() -> {
            test.test();
        }).start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new Thread(() -> {
            test.test();
        }).start();


    }

}
