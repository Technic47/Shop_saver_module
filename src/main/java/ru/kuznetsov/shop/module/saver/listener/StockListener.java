package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.dto.StockDto;
import ru.kuznetsov.shop.data.service.StockService;

@Component
@RequiredArgsConstructor
public class StockListener {

    private final static String STOCK_SAVE_TOPIC = "shop_save_stock";

    private final StockService stockService;

    Logger logger = LoggerFactory.getLogger(StockListener.class);

    @KafkaListener(topics = STOCK_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(StockDto stockDto) {
        logger.info("Saving stock {}", stockDto);
        stockService.add(stockDto);
    }
}
