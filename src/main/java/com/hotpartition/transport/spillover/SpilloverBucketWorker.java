package com.hotpartition.transport.spillover;

import com.hotpartition.reshard.BucketWorker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Owner-side half of transport Option B: instead of handing a record to an in-process
 * thread, produces it to {@code SPILLOVER_TOPIC} partition {@code bucketId} and waits for
 * an async completion ack on {@code COMPLETIONS_TOPIC} partition {@code bucketId} before
 * invoking {@code onComplete} -- so unlike {@link com.hotpartition.reshard.InProcessBucketWorker},
 * {@code processor} passed to {@link #submit} is never actually invoked here; the peer
 * reading the spillover partition is assumed to run equivalent processing logic locally
 * (see {@link SpilloverPeerWorker}). That's a real asymmetry worth flagging, not an
 * oversight: a Java closure can't be shipped across a process boundary, so cross-process
 * delegation only works if the peer is independently running the same application code.
 */
public final class SpilloverBucketWorker implements BucketWorker {

    private static final Logger log = LoggerFactory.getLogger(SpilloverBucketWorker.class);

    private final int bucketId;
    private final TopicPartition originalPartition;
    private final KafkaProducer<String, byte[]> spilloverProducer;
    private final KafkaConsumer<String, String> completionsConsumer;
    private final Map<Long, Runnable> pending = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread pollThread;

    public SpilloverBucketWorker(int bucketId, TopicPartition originalPartition,
                                  KafkaProducer<String, byte[]> spilloverProducer,
                                  KafkaConsumer<String, String> completionsConsumer) {
        this.bucketId = bucketId;
        this.originalPartition = originalPartition;
        this.spilloverProducer = spilloverProducer;
        this.completionsConsumer = completionsConsumer;
    }

    @Override
    public void start() {
        running.set(true);
        TopicPartition completionsPartition = new TopicPartition(SpilloverTopics.COMPLETIONS_TOPIC, bucketId);
        completionsConsumer.assign(List.of(completionsPartition));
        completionsConsumer.seekToBeginning(List.of(completionsPartition));
        pollThread = new Thread(this::runLoop, "spillover-completions-bucket-" + bucketId);
        pollThread.setDaemon(true);
        pollThread.start();
    }

    @Override
    public void submit(ConsumerRecord<String, byte[]> record,
                        BiConsumer<ConsumerRecord<String, byte[]>, Runnable> processor,
                        Runnable onComplete) {
        pending.put(record.offset(), onComplete);
        ProducerRecord<String, byte[]> spilloverRecord = new ProducerRecord<>(
                SpilloverTopics.SPILLOVER_TOPIC, bucketId, record.key(), record.value());
        spilloverRecord.headers()
                .add(new RecordHeader(SpilloverTopics.HEADER_ORIG_TOPIC,
                        originalPartition.topic().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SpilloverTopics.HEADER_ORIG_PARTITION,
                        intToBytes(originalPartition.partition())))
                .add(new RecordHeader(SpilloverTopics.HEADER_ORIG_OFFSET,
                        longToBytes(record.offset())));
        spilloverProducer.send(spilloverRecord, (metadata, exception) -> {
            if (exception != null) {
                log.warn("failed to spill record at offset {} to bucket {}", record.offset(), bucketId, exception);
            }
        });
    }

    private void runLoop() {
        while (running.get()) {
            try {
                ConsumerRecords<String, String> records = completionsConsumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> ack : records) {
                    long originalOffset = Long.parseLong(ack.value());
                    Runnable onComplete = pending.remove(originalOffset);
                    if (onComplete != null) {
                        onComplete.run();
                    }
                    // an ack with no matching pending entry is expected on restart (acks for a
                    // prior run) and is harmless to ignore.
                }
            } catch (org.apache.kafka.common.errors.WakeupException e) {
                // expected on close()
            } catch (RuntimeException e) {
                log.warn("bucket {} completions poll failed", bucketId, e);
            }
        }
    }

    /** Records still awaiting a completion ack. Exposed for tests/observability. */
    public int pendingCount() {
        return pending.size();
    }

    private static byte[] intToBytes(int v) {
        return new byte[]{(byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v};
    }

    private static byte[] longToBytes(long v) {
        byte[] b = new byte[8];
        for (int i = 7; i >= 0; i--) {
            b[i] = (byte) (v & 0xFF);
            v >>= 8;
        }
        return b;
    }

    @Override
    public void close() {
        running.set(false);
        completionsConsumer.wakeup();
        if (pollThread != null) {
            try {
                pollThread.join(Duration.ofSeconds(5).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        completionsConsumer.close();
        spilloverProducer.close();
    }
}
