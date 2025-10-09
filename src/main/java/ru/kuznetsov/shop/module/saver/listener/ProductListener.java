package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.dto.ProductDto;
import ru.kuznetsov.shop.data.service.ProductService;

@Component
@RequiredArgsConstructor
public class ProductListener {

    private final static String PRODUCT_SAVE_TOPIC = "shop_save_product";

    private final ProductService productService;

    Logger logger = LoggerFactory.getLogger(ProductListener.class);

    @KafkaListener(topics = PRODUCT_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(ProductDto productDto) {
        logger.info("Saving product {}", productDto);
        productService.add(productDto);
    }
}
