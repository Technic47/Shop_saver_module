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
import ru.kuznetsov.shop.kafka.service.KafkaService;
import ru.kuznetsov.shop.represent.dto.StockDto;
import ru.kuznetsov.shop.represent.dto.order.BucketItemDto;
import ru.kuznetsov.shop.represent.dto.order.OrderDto;
import ru.kuznetsov.shop.represent.dto.order.OrderStatusDto;
import ru.kuznetsov.shop.represent.dto.order.SellerNotificationDto;
import ru.kuznetsov.shop.represent.enums.OrderStatusType;

import java.util.*;

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
    private final ProductService productService;
    private final KafkaService kafkaService;
    private final ObjectMapper objectMapper;

    Logger logger = LoggerFactory.getLogger(OrderListener.class);

    @Transactional
    @KafkaListener(topics = ORDER_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveOrder(String orderDtoJson, @Header(OPERATION_ID_HEADER) byte[] operationId) {

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
            processStock(bucket, null, customerId, orderId);
            processOrderStatus(orderId, CREATED);
            notifySellers(bucket);

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
            logger.error(e.getMessage());
        }
    }

    private void processBucketItems(Set<BucketItemDto> bucket, Long orderId, UUID customerId) {
        for (BucketItemDto bucketItemDto : bucket) {
            bucketItemDto.setOrderId(orderId);
            bucketItemDto.setCustomerId(customerId.toString());

            if (bucketItemDto.getOwnerId() == null) {
                bucketItemDto.setOwnerId(productService.getOwner(bucketItemDto.getProductId()));
            }

            bucketItemService.add(bucketItemDto);
        }

        logger.info("Buckets for order {} and customer {} saved.", orderId, customerId);
    }

    private void processStock(Set<BucketItemDto> bucket, Long storeId, UUID customerId, Long orderId) {
        logger.info("Processing stock for order {} and customer {}.", orderId, customerId);
        for (BucketItemDto item : bucket) {
            Integer requestedAmount = item.getAmount();
            List<StockDto> allStock = stockService.findAllByOptionalParams(item.getProductId(), storeId, UUID.fromString(item.getOwnerId()))
                    .stream()
                    .filter(stock -> stock.getIsReserved().equals(false))
                    .toList();

            logger.debug("Stock amount for productId: {} is {}. Requested amount: {}", item.getProductId(), allStock.stream().mapToInt(StockDto::getAmount).sum(), requestedAmount);

            for (StockDto stockDto : allStock) {
                logger.debug("StockDto:  id - {}, amount - {}", stockDto.getId(), stockDto.getAmount());
                Integer stockAmount = stockDto.getAmount();

                if (requestedAmount >= stockAmount) {
                    logger.debug("RequestedAmount >= stockAmount. Update to reserved.");

                    stockDto.setIsReserved(true);
                    stockDto.setReservationOrderId(orderId);
                    stockService.update(stockDto);

                    if (requestedAmount.equals(stockAmount)) {
                        logger.debug("RequestedAmount equals stock amount {} - {}. Break stock processing", requestedAmount, stockAmount);
                        break;
                    }

                    requestedAmount -= stockAmount;

                    logger.debug("RequestedAmount remains: {}", requestedAmount);
                } else {
                    logger.debug("RequestedAmount < stockAmount. Split stock.");

                    logger.debug("Creating new stock for productId: {} with amount: {}", item.getProductId(), requestedAmount);
                    stockService.add(new StockDto(requestedAmount,
                            stockDto.getProductId(),
                            stockDto.getProductName(),
                            stockDto.getStore(),
                            stockDto.getStoreAddress(),
                            true,
                            orderId));

                    stockDto.setAmount(stockAmount - requestedAmount);
                    logger.debug("Updating stock with id: {} with amount: {}", stockDto.getId(), stockDto.getAmount());
                    stockService.update(stockDto);

                    break;
                }
            }
        }

        logger.info("Stocks for order {} and customer {} processed.", orderId, customerId);
    }

    private void processOrderStatus(Long orderId, OrderStatusType orderStatus) {
        orderStatusService.add(new OrderStatusDto(orderStatus, null, null, orderId));

        logger.info("Order status for order {} processed.", orderId);
    }

    private void notifySellers(Set<BucketItemDto> bucket) {
        Map<String, SellerNotificationDto> sellerBucketMap = new HashMap<>();

        for (BucketItemDto bucketItemDto : bucket) {
            String ownerId = bucketItemDto.getOwnerId();

            if (sellerBucketMap.containsKey(ownerId)) {
                sellerBucketMap.get(ownerId).getProducts().add(bucketItemDto);
            } else {
                Set<BucketItemDto> sellerProducts = new HashSet<>();
                sellerProducts.add(bucketItemDto);

                sellerBucketMap.put(
                        ownerId,
                        new SellerNotificationDto(
                                null,
                                ownerId,
                                bucketItemDto.getOrderId(),
                                sellerProducts
                        )
                );
            }

            sellerBucketMap.values()
                    .forEach(sellerBucket -> kafkaService.sendMessage(
                            sellerBucket,
                            SELLER_NOTIFICATION_TOPIC,
                            Collections.emptyMap()
                    ));
        }

        logger.info("Sellers notified");
    }
}
