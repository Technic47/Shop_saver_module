package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.service.ProductCategoryService;
import ru.kuznetsov.shop.module.saver.service.DefaultWorkFlowService;
import ru.kuznetsov.shop.represent.dto.ProductCategoryDto;

import static ru.kuznetsov.shop.represent.common.KafkaConst.*;

@Component
@RequiredArgsConstructor
public class ProductCategoryListener {

    private final ProductCategoryService productCategoryService;
    private final DefaultWorkFlowService defaultWorkFlowService;

    Logger logger = LoggerFactory.getLogger(ProductCategoryListener.class);

    @KafkaListener(topics = PRODUCT_CATEGORY_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(String productCategoryDto, @Header(OPERATION_ID_HEADER) byte[] operationId) {

        defaultWorkFlowService.save(
                productCategoryDto,
                operationId,
                logger,
                productCategoryService,
                ProductCategoryDto.class,
                PRODUCT_CATEGORY_SAVE_SUCCESSFUL_TOPIC,
                PRODUCT_CATEGORY_SAVE_FAIL_TOPIC);
    }
}
