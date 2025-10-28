package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.service.StoreService;
import ru.kuznetsov.shop.module.saver.service.ListenerService;

import static ru.kuznetsov.shop.represent.common.KafkaConst.*;

@Component
@RequiredArgsConstructor
public class StoreListener {

    private final StoreService storeService;
    private final ListenerService listenerService;

    @KafkaListener(topics = STORE_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(String storeDto, @Header(OPERATION_ID_HEADER) byte[] operationId) {

        listenerService.save(
                storeDto,
                operationId,
                storeService,
                STORE_SAVE_SUCCESSFUL_TOPIC,
                STORE_SAVE_FAIL_TOPIC);
    }
}
