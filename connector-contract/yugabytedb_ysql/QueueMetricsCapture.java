import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public final class QueueMetricsCapture {
    private QueueMetricsCapture() {}

    private static long numberAttribute(
            MBeanServerConnection connection, ObjectName name, String attribute)
            throws IOException, InstanceNotFoundException, ReflectionException {
        try {
            Object value = connection.getAttribute(name, attribute);
            if (!(value instanceof Number number)) {
                throw new IllegalStateException(attribute + " is not numeric for " + name);
            }
            return number.longValue();
        } catch (AttributeNotFoundException ignored) {
            return -1;
        } catch (javax.management.MBeanException exception) {
            throw new IllegalStateException("cannot read " + attribute + " for " + name, exception);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException(
                    "usage: QueueMetricsCapture host port user password duration_ms interval_ms");
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        long durationMillis = Long.parseLong(args[4]);
        long intervalMillis = Long.parseLong(args[5]);
        if (port <= 0 || durationMillis <= 0 || intervalMillis <= 0) {
            throw new IllegalArgumentException("numeric arguments must be positive");
        }

        JMXServiceURL url = new JMXServiceURL(
                "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi");
        Map<String, Object> environment = new HashMap<>();
        environment.put(JMXConnector.CREDENTIALS, new String[] {args[2], args[3]});
        long deadline = System.nanoTime() + durationMillis * 1_000_000L;
        boolean emitted = false;
        try (JMXConnector connector = JMXConnectorFactory.connect(url, environment)) {
            MBeanServerConnection connection = connector.getMBeanServerConnection();
            ObjectName query = new ObjectName("*:type=connector-metrics,context=streaming,*");
            System.out.println(
                    "epoch_ms\tmonotonic_ns\tobject_name\tqueue_total_capacity\t"
                            + "queue_remaining_capacity\tcurrent_queue_size_bytes\tmax_queue_size_bytes");
            System.out.flush();
            while (System.nanoTime() < deadline) {
                Set<ObjectName> names = connection.queryNames(query, null);
                for (ObjectName name : names) {
                    long total = numberAttribute(connection, name, "QueueTotalCapacity");
                    long remaining = numberAttribute(connection, name, "QueueRemainingCapacity");
                    if (total < 0 || remaining < 0) {
                        continue;
                    }
                    System.out.printf(
                            "%d\t%d\t%s\t%d\t%d\t%d\t%d%n",
                            System.currentTimeMillis(),
                            System.nanoTime(),
                            name.getCanonicalName(),
                            total,
                            remaining,
                            numberAttribute(connection, name, "CurrentQueueSizeInBytes"),
                            numberAttribute(connection, name, "MaxQueueSizeInBytes"));
                    System.out.flush();
                    emitted = true;
                }
                Thread.sleep(intervalMillis);
            }
        }
        if (!emitted) {
            throw new IllegalStateException(
                    "no Debezium streaming queue-capacity metric was observed; runtime="
                            + ManagementFactory.getRuntimeMXBean().getName());
        }
    }
}
