package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.service.StoreService;
import ru.kuznetsov.shop.module.saver.service.DefaultWorkFlowService;
import ru.kuznetsov.shop.represent.dto.ProductDto;

import static ru.kuznetsov.shop.represent.common.KafkaConst.OPERATION_ID_HEADER;
import static ru.kuznetsov.shop.represent.common.KafkaConst.STORE_SAVE_TOPIC;

@Component
@RequiredArgsConstructor
public class StoreListener {

    private final StoreService storeService;
    private final DefaultWorkFlowService defaultWorkFlowService;

    Logger logger = LoggerFactory.getLogger(StoreListener.class);

    @KafkaListener(topics = STORE_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(String storeDto, @Header(OPERATION_ID_HEADER) byte[] operationId) {

        defaultWorkFlowService.save(
                storeDto,
                operationId,
                logger,
                storeService,
                ProductDto.class);
    }
}
