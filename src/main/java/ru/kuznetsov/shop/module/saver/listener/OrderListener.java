package ru.kuznetsov.shop.module.saver.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.service.*;
import ru.kuznetsov.shop.represent.dto.StockDto;
import ru.kuznetsov.shop.represent.dto.order.BucketItemDto;
import ru.kuznetsov.shop.represent.dto.order.OrderDto;
import ru.kuznetsov.shop.represent.dto.order.OrderStatusDto;
import ru.kuznetsov.shop.represent.enums.OrderStatusType;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static ru.kuznetsov.shop.represent.common.KafkaConst.*;
import static ru.kuznetsov.shop.represent.enums.OrderStatusType.CREATED;
import static ru.kuznetsov.shop.represent.enums.OrderStatusType.ERROR;

@Component
@RequiredArgsConstructor
public class OrderListener {

    private final OrderService orderService;
    private final OrderStatusService orderStatusService;
    private final BucketItemService bucketItemService;
    private final StockService stockService;
    private final KafkaService kafkaService;
    private final ObjectMapper objectMapper;

    Logger logger = LoggerFactory.getLogger(OrderListener.class);

    @Transactional
    @KafkaListener(topics = ORDER_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(String orderDtoJson, @Header(OPERATION_ID_HEADER) byte[] operationId) {

        String operationIdEncoded = new String(operationId);
        Long orderId = null;

        logger.info("Saving order {} with operationId: {}", orderDtoJson, operationIdEncoded);

        try {
            OrderDto orderDto = objectMapper.readValue(orderDtoJson, OrderDto.class);
            UUID customerId = UUID.fromString(orderDto.getCustomerId());
            Set<BucketItemDto> bucket = orderDto.getBucket();

            OrderDto savedOrder = orderService.add(orderDto);
            orderId = savedOrder.getId();
            processBucketItems(bucket, savedOrder.getId(), customerId);
            processStock(bucket, null, customerId);
            processOrderStatus(orderId, CREATED);

            kafkaService.sendMessageWithEntity(savedOrder,
                    ORDER_SAVE_SUCCESSFUL_TOPIC,
                    Collections.singletonMap(OPERATION_ID_HEADER, operationId));

            logger.info("Order saved. Id: {}, operationId: {}", savedOrder.getId(), operationIdEncoded);
        } catch (Exception e) {
            if (orderId != null) processOrderStatus(orderId, ERROR);

            kafkaService.sendMessage(orderDtoJson,
                    ORDER_SAVE_FAIL_TOPIC,
                    Collections.singletonMap(OPERATION_ID_HEADER, operationId));

            logger.error("Order saving failed. OperationId: {}, order: {}, ", operationIdEncoded, orderDtoJson);
        }
    }

    private void processBucketItems(Set<BucketItemDto> bucket, Long orderId, UUID customerId) {
        for (BucketItemDto bucketItemDto : bucket) {
            bucketItemDto.setOrderId(orderId);
            bucketItemDto.setCustomerId(customerId.toString());

            bucketItemService.add(bucketItemDto);
        }

        logger.info("Buckets for order {} and customer {} saved.", orderId, customerId);
    }

    private void processStock(Set<BucketItemDto> bucket, Long storeId, UUID customerId) {
        for (BucketItemDto item : bucket) {
            Integer requestedAmount = item.getAmount();
            List<StockDto> allStock = stockService.findAllByOptionalParams(item.getProductId(), storeId, customerId);

            for (StockDto stockDto : allStock) {
                Integer stockAmount = stockDto.getAmount();

                if (requestedAmount >= stockAmount) {
                    stockDto.setIsReserved(true);
                    stockService.update(stockDto);

                    if (requestedAmount.equals(stockAmount)) break;

                    requestedAmount -= stockAmount;
                } else {
                    stockService.add(new StockDto(requestedAmount,
                            stockDto.getProductId(),
                            stockDto.getProductName(),
                            stockDto.getStore(),
                            stockDto.getStoreAddress(),
                            true));

                    stockDto.setAmount(stockAmount - requestedAmount);
                    stockService.update(stockDto);

                    break;
                }
            }
        }
    }

    private void processOrderStatus(Long orderId, OrderStatusType orderStatus) {
        orderStatusService.add(new OrderStatusDto(orderStatus, null, null, orderId));
    }
}
