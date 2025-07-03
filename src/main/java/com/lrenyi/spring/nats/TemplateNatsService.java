package com.lrenyi.spring.nats;

import com.lrenyi.template.core.util.StringUtils;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TemplateNatsService {
    
    private final ConnectionHolder connectionHolder;
    
    public TemplateNatsService(ConnectionHolder connectionHolder) {
        this.connectionHolder = connectionHolder;
    }
    
    public Response publishEvent(@NonNull String subject, @NonNull String eventName, String jsonBody) {
        return publishEvent(subject, eventName, jsonBody, 30);
    }
    
    public Response publishEvent(@NonNull String subject, @NonNull String eventName, String jsonBody, int timeout) {
        ByteBuf eventBuf = Unpooled.buffer();
        byte[] codeBytes = eventName.getBytes(StandardCharsets.UTF_8);
        eventBuf.writeByte(codeBytes.length);
        eventBuf.writeBytes(codeBytes);
        if (StringUtils.hasLength(jsonBody)) {
            byte[] jsonBodyBytes = jsonBody.getBytes(StandardCharsets.UTF_8);
            eventBuf.writeInt(jsonBodyBytes.length);
            eventBuf.writeBytes(jsonBodyBytes);
        } else {
            eventBuf.writeInt(0);
        }
        Optional<Connection> connectionOptional = connectionHolder.getValidateConnection();
        if (connectionOptional.isEmpty()) {
            throw new RuntimeException("the connection of nats is null when publish event.");
        }
        Connection connection = connectionOptional.get();
        String replay = connection.createInbox();
        connection.publish(subject, replay, eventBuf.array());
        Subscription subscribe = connection.subscribe(replay);
        Message message;
        try {
            message = subscribe.nextMessage(Duration.ofSeconds(timeout));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Response response = new Response();
        byte[] messageData = message.getData();
        ByteBuf resBuf = Unpooled.wrappedBuffer(messageData);
        int readableBytes = resBuf.readableBytes();
        if (readableBytes < 5) {
            throw new RuntimeException("Protocol format exception in response data.");
        }
        int success = resBuf.readByte();
        response.setSuccess(success == 1);
        int bodySize = resBuf.readInt();
        readableBytes = resBuf.readableBytes();
        if (readableBytes < bodySize) {
            throw new RuntimeException("Protocol format exception in body data of response");
        }
        ByteBuf byteBuf = resBuf.readBytes(bodySize);
        response.setData(byteBuf.toString(StandardCharsets.UTF_8));
        byteBuf.release();
        return response;
    }
    
    public void handOriginalMessage(Message message) {
        byte[] data = message.getData();
        if (data.length <= 5) {
            throw new IllegalArgumentException("Received abnormal NATS data with data length <= 5");
        }
        ByteBuf bodyBuf = Unpooled.wrappedBuffer(data);
        int nameLength = bodyBuf.readByte();
        int readableBytes = bodyBuf.readableBytes();
        if (readableBytes < nameLength) {
            String info = String.format(
                    "Received abnormal message, event name length is not the expected size, expected:%s, actual:%s",
                    nameLength,
                    readableBytes
            );
            throw new IllegalArgumentException(info);
        }
        ByteBuf nameByteBuf = bodyBuf.readBytes(nameLength);
        String eventName = nameByteBuf.toString(StandardCharsets.UTF_8);
        EventProcessor processor = EventProcessor.ALL_EVENT_PROCESSOR.get(eventName);
        if (processor == null) {
            String info = String.format(
                    "Received message event:%s, but the corresponding message processor was not found.",
                    eventName
            );
            throw new IllegalArgumentException(info);
        }
        int bodyLength = bodyBuf.readInt();
        readableBytes = bodyBuf.readableBytes();
        if (readableBytes < bodyLength) {
            String info = String.format(
                    "Received abnormal message, event body length is not the expected size, expected: %s, actual: %s",
                    bodyLength,
                    readableBytes
            );
            throw new IllegalArgumentException(info);
        }
        String jsonBody = null;
        if (bodyLength != 0) {
            jsonBody = bodyBuf.readBytes(bodyLength).toString(StandardCharsets.UTF_8);
        }
        Connection connection = message.getConnection();
        String replyTo = message.getReplyTo();
        String response;
        boolean success = true;
        try {
            response = processor.handler(jsonBody, connection);
        } catch (Throwable e) {
            success = false;
            response = e.getMessage();
            log.error("", e);
        }
        if (StringUtils.hasLength(replyTo)) {
            ByteBuf responseBuf = Unpooled.buffer();
            if (success) {
                responseBuf.writeByte(1);
            } else {
                responseBuf.writeByte(0);
            }
            if (StringUtils.hasLength(response)) {
                byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
                responseBuf.writeInt(responseBytes.length);
                responseBuf.writeBytes(responseBytes);
            } else {
                responseBuf.writeInt(0);
            }
            connection.publish(replyTo, responseBuf.array());
        }
    }
}