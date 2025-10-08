package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.dto.StoreDto;
import ru.kuznetsov.shop.data.service.StoreService;

@Component
@RequiredArgsConstructor
public class StoreListener {

    private final static String STORE_SAVE_TOPIC = "shop_save_store";

    private final StoreService storeService;

    Logger logger = LoggerFactory.getLogger(StoreListener.class);

    @KafkaListener(topics = STORE_SAVE_TOPIC, groupId = "${spring.kafka.consumer.groupId}")
    public void saveProduct(StoreDto storeDto) {
        logger.info("Saving store {}", storeDto);
        storeService.add(storeDto);
    }
}
