package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.dto.StoreDto;
import ru.kuznetsov.shop.data.service.StoreService;

import static ru.kuznetsov.shop.data.common.KafkaTopics.STORE_SAVE_TOPIC;

@Component
@RequiredArgsConstructor
public class StoreListener {

    private final StoreService storeService;

    Logger logger = LoggerFactory.getLogger(StoreListener.class);

    @KafkaListener(topics = STORE_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(StoreDto storeDto) {
        logger.info("Saving store {}", storeDto);
        storeService.add(storeDto);
    }
}
