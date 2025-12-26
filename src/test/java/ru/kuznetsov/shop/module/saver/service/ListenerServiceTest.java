package ru.kuznetsov.shop.module.saver.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import ru.kuznetsov.shop.data.service.ProductService;
import ru.kuznetsov.shop.kafka.service.KafkaService;
import ru.kuznetsov.shop.represent.dto.ProductDto;

import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ListenerServiceTest {

    private final static String SUCCESSFUL_TOPIC = "successful";
    private final static String FAIL_TOPIC = "fail";

    private ListenerService listenerService;
    protected ObjectMapper om = new ObjectMapper();

    @MockitoBean
    private ProductService productService;
    @MockitoBean
    private KafkaService kafkaService;

    @BeforeEach
    void setUp() {
        listenerService = new ListenerService(kafkaService, om);
    }

    @Test
    void saveSuccess() throws JsonProcessingException {
        ProductDto mockDto = getMockDto();
        String mockJson = om.writeValueAsString(mockDto);

        doReturn(new ProductDto())
                .when(productService)
                .add(any());
        doReturn(true)
                .when(kafkaService)
                .sendMessageWithEntity(any(ProductDto.class), eq(SUCCESSFUL_TOPIC), any(Map.class));

        listenerService.save(
                mockJson,
                UUID.randomUUID().toString().getBytes(),
                productService,
                SUCCESSFUL_TOPIC,
                "fail",
                mockDto.getClass());

        verify(productService, times(1)).add(any());
        verify(kafkaService, times(1)).sendMessageWithEntity(any(ProductDto.class), eq(SUCCESSFUL_TOPIC), any(Map.class));
        verify(kafkaService, times(0)).sendMessage(eq(mockJson), eq(FAIL_TOPIC), any(Map.class));
    }

    @Test
    void saveFail() throws JsonProcessingException {
        ProductDto mockDto = getMockDto();
        String mockJson = om.writeValueAsString(mockDto);

        doThrow(RuntimeException.class)
                .when(productService)
                .add(any());
        doReturn(true)
                .when(kafkaService)
                .sendMessageWithEntity(any(ProductDto.class), eq(FAIL_TOPIC), any(Map.class));

        listenerService.save(
                mockJson,
                UUID.randomUUID().toString().getBytes(),
                productService,
                SUCCESSFUL_TOPIC,
                "fail",
                mockDto.getClass()
        );

        verify(productService, times(1)).add(any());
        verify(kafkaService, times(0)).sendMessageWithEntity(any(ProductDto.class), eq(SUCCESSFUL_TOPIC), any(Map.class));
        verify(kafkaService, times(1)).sendMessage(eq(mockJson), eq(FAIL_TOPIC), any(Map.class));
        verifyNoMoreInteractions(kafkaService);
    }

    private ProductDto getMockDto() {
        ProductDto dto = new ProductDto();
        dto.setName("Test");
        dto.setDescription("Test");
        dto.setPrice(123);

        return dto;
    }
}