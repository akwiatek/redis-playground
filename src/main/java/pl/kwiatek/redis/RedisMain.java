package pl.kwiatek.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static pl.kwiatek.redis.CancelledTestRunMessage.CHANNEL;

@Slf4j
public class RedisMain {
    private static final CancelledTestRunMessage SAMPLE_MESSAGE = new CancelledTestRunMessage("wallmart", 20L);
    private static final String REDIS_URI = "redis://localhost";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String... args) throws InterruptedException, BrokenBarrierException {
        new RedisMain().run();
    }

    private void run() throws InterruptedException, BrokenBarrierException {
        var barrier = new CyclicBarrier(2);
        withClient(REDIS_URI, client -> whenSubscribed(client, CHANNEL, barrier, () -> withConnection(client, connection -> {
            var json = cancelledTestRunMessage();
            var syncCommands = connection.sync();
            barrier.await();
            log.info("Publishing JSON");
            syncCommands.publish(CHANNEL, json);
            log.info("JSON published");
            barrier.await();
        })));
    }

    private void withClient(String uri, InterruptableConsumer<RedisClient> consumer) throws InterruptedException, BrokenBarrierException {
        RedisClient client = null;
        try {
            client = RedisClient.create(uri);
            consumer.accept(client);
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    private void whenSubscribed(RedisClient client, String channel, CyclicBarrier barrier, InterruptableRunnable action) throws InterruptedException, BrokenBarrierException {
        String[] channels = { channel };
        try (var subscribingThread = new RedisSubscribingThread(client, channels, barrier)) {
            subscribingThread.start();
            action.run();
        }
    }

    private void withConnection(RedisClient client, InterruptableConsumer<StatefulRedisConnection<String, String>> action) throws InterruptedException, BrokenBarrierException {
        try (var connection = client.connect()) {
            action.accept(connection);
        }
    }

    private String cancelledTestRunMessage() {
        try {
            return MAPPER.writeValueAsString(SAMPLE_MESSAGE);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private interface InterruptableConsumer<T> {
        void accept(T t) throws InterruptedException, BrokenBarrierException;
    }

    private interface InterruptableRunnable {
        void run() throws InterruptedException, BrokenBarrierException;
    }
}
