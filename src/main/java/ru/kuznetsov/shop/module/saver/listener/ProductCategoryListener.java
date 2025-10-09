package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.dto.ProductCategoryDto;
import ru.kuznetsov.shop.data.service.ProductCategoryService;

import static ru.kuznetsov.shop.data.common.KafkaTopics.PRODUCT_CATEGORY_SAVE_TOPIC;

@Component
@RequiredArgsConstructor
public class ProductCategoryListener {

    private final ProductCategoryService productCategoryService;

    Logger logger = LoggerFactory.getLogger(ProductCategoryListener.class);

    @KafkaListener(topics = PRODUCT_CATEGORY_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(ProductCategoryDto productCategoryDto) {
        logger.info("Saving product category {}", productCategoryDto);
        productCategoryService.add(productCategoryDto);
    }
}
