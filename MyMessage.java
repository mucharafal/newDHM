import java.io.Serializable;

public class MyMessage<K, V> implements Serializable {
    public K key;
    public V value;
    public RequestType type;

    public MyMessage(RequestType type, K key, V value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }
    public MyMessage() {}
}