package pl.kwiatek.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

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
        try (StatefulRedisPubSubConnection<String, String> connection = redisClient.connectPubSub()) {
            connection.addListener(new LoggingRedisPubSubAdapter());
            barrier.await();
            connection.sync().subscribe(channels);
            sleepUntilShutDown();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
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

    private static class LoggingRedisPubSubAdapter extends RedisPubSubAdapter<String, String> {
        @Override
        public void message(String channel, String message) {
            System.out.println("onMessage, channel:" + channel + ", message: " + message);
            CancelledTestRunMessage msgObject;
            try {
                msgObject = MAPPER.readValue(message, CancelledTestRunMessage.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            System.out.println("msgObject: " + msgObject);
        }
    }
}
