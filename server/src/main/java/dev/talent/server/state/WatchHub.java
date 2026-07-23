package dev.talent.server.state;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public final class WatchHub {
    public enum Type { PUT, DELETE }

    public record Event(Type type, KvRecord current, KvRecord previous) {}

    public interface Handle extends AutoCloseable {
        long id();
        @Override void close();
    }

    private final Executor deliveryExecutor;
    private final int queueCapacity;
    private final AtomicLong ids = new AtomicLong();
    private final Map<Long, Subscription> subscriptions = new ConcurrentHashMap<>();
    private final ArrayDeque<Event> history = new ArrayDeque<>();
    private volatile long compactedRevision;

    public WatchHub(Executor deliveryExecutor, int queueCapacity) {
        this.deliveryExecutor = deliveryExecutor;
        this.queueCapacity = queueCapacity;
    }

    public Handle subscribe(
            ByteKey start,
            ByteKey endExclusive,
            long startRevision,
            boolean includePrevious,
            Consumer<List<Event>> consumer,
            Consumer<Throwable> errorConsumer) {
        if (startRevision > 0 && startRevision <= compactedRevision) {
            throw new CompactedException(compactedRevision);
        }
        long id = ids.incrementAndGet();
        Subscription subscription = new Subscription(
                id, start, endExclusive, includePrevious, consumer, errorConsumer);
        subscriptions.put(id, subscription);
        List<Event> replay;
        synchronized (history) {
            replay = history.stream()
                    .filter(event -> event.current().modificationRevision() >= startRevision)
                    .filter(subscription::matches)
                    .map(event -> includePrevious
                            ? event
                            : new Event(event.type(), event.current(), null))
                    .toList();
        }
        if (!replay.isEmpty()) {
            subscription.offer(replay);
        }
        return subscription;
    }

    /** Called only after the coordinator mutation lock has been released. */
    public void publish(List<Event> events) {
        if (events.isEmpty()) {
            return;
        }
        synchronized (history) {
            history.addAll(events);
        }
        subscriptions.values().forEach(subscription -> {
            List<Event> matching = events.stream()
                    .filter(subscription::matches)
                    .map(event -> subscription.includePrevious
                            ? event
                            : new Event(event.type(), event.current(), null))
                    .toList();
            if (!matching.isEmpty()) {
                subscription.offer(matching);
            }
        });
    }

    public void compact(long revision) {
        compactedRevision = revision;
        synchronized (history) {
            while (!history.isEmpty()
                    && history.getFirst().current().modificationRevision() <= revision) {
                history.removeFirst();
            }
        }
    }

    public void reset(long compactedRevision) {
        this.compactedRevision = compactedRevision;
        synchronized (history) {
            history.clear();
        }
        subscriptions.values().forEach(subscription ->
                subscription.fail(new IllegalStateException("state machine restored")));
        subscriptions.clear();
    }

    private final class Subscription implements Handle {
        private final long id;
        private final ByteKey start;
        private final ByteKey endExclusive;
        private final boolean includePrevious;
        private final Consumer<List<Event>> consumer;
        private final Consumer<Throwable> errorConsumer;
        private final ArrayDeque<List<Event>> queue = new ArrayDeque<>();
        private final AtomicBoolean draining = new AtomicBoolean();
        private volatile boolean closed;

        private Subscription(
                long id,
                ByteKey start,
                ByteKey endExclusive,
                boolean includePrevious,
                Consumer<List<Event>> consumer,
                Consumer<Throwable> errorConsumer) {
            this.id = id;
            this.start = start;
            this.endExclusive = endExclusive;
            this.includePrevious = includePrevious;
            this.consumer = consumer;
            this.errorConsumer = errorConsumer;
        }

        @Override
        public long id() {
            return id;
        }

        private boolean matches(Event event) {
            int lower = event.current().key().compareTo(start);
            return lower >= 0
                    && (endExclusive == null || event.current().key().compareTo(endExclusive) < 0);
        }

        private void offer(List<Event> batch) {
            synchronized (queue) {
                if (closed) {
                    return;
                }
                if (queue.size() >= queueCapacity) {
                    fail(new IllegalStateException("watch consumer is too slow"));
                    return;
                }
                queue.addLast(List.copyOf(batch));
            }
            schedule();
        }

        private void schedule() {
            if (draining.compareAndSet(false, true)) {
                deliveryExecutor.execute(this::drain);
            }
        }

        private void drain() {
            try {
                while (!closed) {
                    List<Event> batch;
                    synchronized (queue) {
                        batch = queue.pollFirst();
                    }
                    if (batch == null) {
                        return;
                    }
                    consumer.accept(batch);
                }
            } catch (Throwable problem) {
                fail(problem);
            } finally {
                draining.set(false);
                synchronized (queue) {
                    if (!closed && !queue.isEmpty()) {
                        schedule();
                    }
                }
            }
        }

        private void fail(Throwable problem) {
            close();
            errorConsumer.accept(problem);
        }

        @Override
        public void close() {
            closed = true;
            subscriptions.remove(id, this);
            synchronized (queue) {
                queue.clear();
            }
        }
    }
}
