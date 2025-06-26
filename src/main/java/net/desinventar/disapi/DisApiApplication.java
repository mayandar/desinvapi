package net.desinventar.disapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
//import org.springframework.boot.autoconfigure.jdbc.XADataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/* Consolidated controller - Rest service configuration.
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.0
 * */
@SpringBootApplication(exclude={DataSourceAutoConfiguration.class})
//@EnableAutoConfiguration(exclude={DataSourceAutoConfiguration.class, XADataSourceAutoConfiguration.class})
@EnableConfigurationProperties(ConfigProperties.class)
public class DisApiApplication {
    //private static final Logger log = LoggerFactory.getLogger(DisApiApplication.class);
    public static void main(String[] args) {
    	SpringApplication.run(DisApiApplication.class, args);
    }
}
