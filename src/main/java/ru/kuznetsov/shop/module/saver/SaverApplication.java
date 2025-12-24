package ru.kuznetsov.shop.module.saver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import ru.kuznetsov.shop.data.config.SpringConfig;
import ru.kuznetsov.shop.kafka.config.KafkaConfig;

@SpringBootApplication
@Import({SpringConfig.class, KafkaConfig.class})
public class SaverApplication {

    public static void main(String[] args) {
        SpringApplication.run(SaverApplication.class, args);
    }

}
