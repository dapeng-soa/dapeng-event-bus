/**
 * desc: TODO
 *
 * @author hz.lei
 * @since 2018年05月26日 下午8:53
 */
public class Test1 {

    public static void main(String[] args) {
        String ss = "member-instance-1";

        String s1 = ss.substring(0,ss.indexOf("-"));
        String s2 = ss.substring(ss.indexOf("-")+1);
        System.out.println(s1+"----   "+s2);

    }
}
