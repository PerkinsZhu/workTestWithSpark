package dubbo;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;

/**
 * Created by PerkinsZhu on 2018/8/30 16:10
 **/
public class RemoteConsumer {
    public static void main(String[] args) {
        ReferenceConfig<GreetingService> referenceConfig = new ReferenceConfig<GreetingService>();
        ApplicationConfig aconfig = new ApplicationConfig("first-dubbo-consumer");
        aconfig.setQosPort(8868);
        referenceConfig.setApplication(aconfig);
        RegistryConfig config = new RegistryConfig("zookeeper://192.168.10.156:2181");
        referenceConfig.setRegistry(config);
        referenceConfig.setInterface(GreetingService.class);
        GreetingService greetingService = referenceConfig.get();
        System.out.println(greetingService.sayHello("world"));
        referenceConfig.getClient();
    }
}
