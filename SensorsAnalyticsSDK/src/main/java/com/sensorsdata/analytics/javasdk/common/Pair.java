package com.sensorsdata.analytics.javasdk.common;

/**
 * 生成一个 KV 对象
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/03/12 16:37
 */
public class Pair<K, V> {

  private final K key;

  private final V value;

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  private Pair(K key, V value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public String toString() {
    return key + "=" + value;
  }


  public boolean equals(Object var1) {
    return var1 instanceof Pair
        && ((Pair<?, ?>) var1).getKey() == this.key
        && this.value == ((Pair<?, ?>) var1).getValue();
  }

  public int hashCode() {
    if (this.key == null) {
      return this.value == null ? 0 : this.value.hashCode() + 1;
    } else {
      return this.value == null ? this.key.hashCode() + 2 : this.key.hashCode() * 17 + this.value.hashCode();
    }
  }

  public static <K, V> Pair<K, V> of(K key, V value) {
    return new Pair<>(key, value);
  }

}
