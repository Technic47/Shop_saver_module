package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.service.ProductService;
import ru.kuznetsov.shop.module.saver.service.DefaultWorkFlowService;
import ru.kuznetsov.shop.represent.dto.ProductDto;

import static ru.kuznetsov.shop.represent.common.KafkaConst.OPERATION_ID_HEADER;
import static ru.kuznetsov.shop.represent.common.KafkaConst.PRODUCT_SAVE_TOPIC;

@Component
@RequiredArgsConstructor
public class ProductListener {

    private final ProductService productService;
    private final DefaultWorkFlowService defaultWorkFlowService;

    Logger logger = LoggerFactory.getLogger(ProductListener.class);

    @KafkaListener(topics = PRODUCT_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(String productDto, @Header(OPERATION_ID_HEADER) byte[] operationId) {

        defaultWorkFlowService.save(
                productDto,
                operationId,
                logger,
                productService,
                ProductDto.class);
    }
}
