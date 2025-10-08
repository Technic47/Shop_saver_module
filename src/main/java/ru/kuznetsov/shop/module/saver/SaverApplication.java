package ru.kuznetsov.shop.module.saver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import ru.kuznetsov.shop.data.config.SpringConfig;

@SpringBootApplication
@Import(SpringConfig.class)
public class SaverApplication {

    public static void main(String[] args) {
        SpringApplication.run(SaverApplication.class, args);
    }

}
