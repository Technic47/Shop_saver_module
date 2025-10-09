package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.dto.ProductDto;
import ru.kuznetsov.shop.data.service.ProductService;

import static ru.kuznetsov.shop.data.common.KafkaTopics.PRODUCT_SAVE_TOPIC;

@Component
@RequiredArgsConstructor
public class ProductListener {

    private final ProductService productService;

    Logger logger = LoggerFactory.getLogger(ProductListener.class);

    @KafkaListener(topics = PRODUCT_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(ProductDto productDto) {
        logger.info("Saving product {}", productDto);
        productService.add(productDto);
    }
}
