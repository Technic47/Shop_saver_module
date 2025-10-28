package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.service.ProductCategoryService;
import ru.kuznetsov.shop.module.saver.service.ListenerService;

import static ru.kuznetsov.shop.represent.common.KafkaConst.*;

@Component
@RequiredArgsConstructor
public class ProductCategoryListener {

    private final ProductCategoryService productCategoryService;
    private final ListenerService listenerService;

    @KafkaListener(topics = PRODUCT_CATEGORY_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(String productCategoryDto, @Header(OPERATION_ID_HEADER) byte[] operationId) {

        listenerService.save(
                productCategoryDto,
                operationId,
                productCategoryService,
                PRODUCT_CATEGORY_SAVE_SUCCESSFUL_TOPIC,
                PRODUCT_CATEGORY_SAVE_FAIL_TOPIC);
    }
}
