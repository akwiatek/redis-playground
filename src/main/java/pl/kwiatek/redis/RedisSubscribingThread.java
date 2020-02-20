package pl.kwiatek.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

@Slf4j
@RequiredArgsConstructor
public class RedisSubscribingThread extends Thread implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RedisClient redisClient;
    private final String[] channels;
    private final CyclicBarrier barrier;

    private volatile boolean shuttingDown;

    @Override
    public void close() throws InterruptedException {
        shuttingDown = true;
        interrupt();
        join();
    }

    public void run() {
        log.info("Connecting to Redis");
        try (StatefulRedisPubSubConnection<String, String> connection = redisClient.connectPubSub()) {
            log.info("Connected to Redis");
            connection.addListener(new LoggingRedisPubSubAdapter(barrier));
            connection.sync().subscribe(channels);
            log.info("Subscribed to {}", channels);
            barrier.await();
            sleepUntilShutDown();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
        log.info("Disconnected from Redis");
    }

    private void sleepUntilShutDown() throws InterruptedException {
        while (!shuttingDown) {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                if (shuttingDown) {
                    break;
                }
                throw e;
            }
        }
    }

    @RequiredArgsConstructor
    private static class LoggingRedisPubSubAdapter extends RedisPubSubAdapter<String, String> {

        private final CyclicBarrier barrier;

        @Override
        public void message(String channel, String message) {
            log.info("Message received");
            log.info("channel: {}", channel);
            log.info("message: {}", message);
            CancelledTestRunMessage msgObject;
            try {
                msgObject = MAPPER.readValue(message, CancelledTestRunMessage.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            log.info("msgObject: {}", msgObject);
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
