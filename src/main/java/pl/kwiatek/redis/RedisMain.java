package pl.kwiatek.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;

import static java.lang.Thread.sleep;

/**
 * Hello world!
 *
 */
public class RedisMain {
    private static final String REDIS_URI = "redis://localhost";
    private static final String CHANNEL = "test-run-cancels";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String... args) throws InterruptedException {
        new RedisMain().run();
    }

    private void run() throws InterruptedException {
        String json = cancelledTestRunMessage("wallmart", 20L);
        withClient(REDIS_URI, client -> whenSubscribed(client, CHANNEL, () -> withConnection(client, connection -> {
            var syncCommands = connection.sync();
            sleep(1000L);
            syncCommands.publish(CHANNEL, json);
            sleep(1000L);
        })));
    }

    private void withClient(String uri, InterruptableConsumer<RedisClient> consumer) throws InterruptedException {
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

    private void whenSubscribed(RedisClient client, String channel, InterruptableRunnable action) throws InterruptedException {
        String[] channels = { channel };
        try (var subscribingThread = new RedisSubscribingThread(client, channels)) {
            subscribingThread.start();
            action.run();
        }
    }

    private void withConnection(RedisClient client, InterruptableConsumer<StatefulRedisConnection<String, String>> action) throws InterruptedException {
        try (var connection = client.connect()) {
            action.accept(connection);
        }
    }

    private String cancelledTestRunMessage(String tenant, long testRunId) {
        var message = new CancelledTestRunMessage(tenant, testRunId);
        try {
            return MAPPER.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private interface InterruptableConsumer<T> {
        void accept(T t) throws InterruptedException;
    }

    private interface InterruptableRunnable {
        void run() throws InterruptedException;
    }
}
