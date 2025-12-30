package ru.kuznetsov.shop.module.saver.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import ru.kuznetsov.shop.kafka.service.MessageCacheService;
import ru.kuznetsov.shop.represent.dto.AbstractDto;

@Configuration
@EnableTransactionManagement
public class SaverConfig {
    @Bean
    public MessageCacheService<AbstractDto> getMessageCacheService() {
        return new MessageCacheService<>();
    }
}
