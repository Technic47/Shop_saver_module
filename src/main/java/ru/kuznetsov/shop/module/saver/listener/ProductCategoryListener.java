package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.dto.ProductCategoryDto;
import ru.kuznetsov.shop.data.service.ProductCategoryService;

@Component
@RequiredArgsConstructor
public class ProductCategoryListener {

    private final static String PRODUCT_CATEGORY_SAVE_TOPIC = "shop_save_product_category";

    private final ProductCategoryService productCategoryService;

    Logger logger = LoggerFactory.getLogger(ProductCategoryListener.class);

    @KafkaListener(topics = PRODUCT_CATEGORY_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(ProductCategoryDto productCategoryDto) {
        logger.info("Saving product category {}", productCategoryDto);
        productCategoryService.add(productCategoryDto);
    }
}
