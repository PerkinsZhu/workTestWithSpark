package dubbo;

/**
 * Created by PerkinsZhu on 2018/8/30 16:08
 **/
public class GreetingServiceImpl implements GreetingService {
    @Override
    public String sayHello(String name) {
        return "hello \t"+name;
    }
}
