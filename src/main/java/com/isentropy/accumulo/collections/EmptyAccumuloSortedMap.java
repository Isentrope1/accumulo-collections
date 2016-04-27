package com.isentropy.accumulo.collections;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;

import com.isentropy.accumulo.collections.io.JavaSerializationSerde;
import com.isentropy.accumulo.collections.io.SerDe;

public class EmptyAccumuloSortedMap<K,V> implements AccumuloSortedMapInterface<K, V> {

	public EmptyAccumuloSortedMap() {
	}

	@Override
	public Comparator<? super K> comparator() {
		return new Comparator(){
			@Override
			public int compare(Object o1, Object o2) {
				return 0;
			}
		};
	}

	@Override
	public SortedMap<K, V> subMap(K fromKey, K toKey) {
		return this;
	}

	@Override
	public SortedMap<K, V> headMap(K toKey) {
		return this;
	}

	@Override
	public SortedMap<K, V> tailMap(K fromKey) {
		return this;
	}

	@Override
	public K firstKey() {
		return null;
	}

	@Override
	public K lastKey() {
		return null;
	}

	@Override
	public Set<K> keySet() {
		return Collections.emptySet();
	}

	@Override
	public Collection<V> values() {
		return Collections.emptySet();
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return Collections.emptySet();
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public boolean containsKey(Object key) {
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		return false;
	}

	@Override
	public V get(Object key) {
		return null;
	}

	@Override
	public V put(K key, V value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V remove(Object key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SerDe getKeySerde() {
		return new JavaSerializationSerde();
	}

	@Override
	public AccumuloSortedMapInterface<K, V> setKeySerde(SerDe s) {
		return this;
	}

	@Override
	public SerDe getValueSerde() {
		return new JavaSerializationSerde();
	}

	@Override
	public AccumuloSortedMapInterface<K, V> setValueSerde(SerDe s) {
		return this;
	}

	@Override
	public String getTable() {
		return null;
	}

	@Override
	public AccumuloSortedMapInterface<K, V> setColumnVisibility(byte[] cv) {
		return this;
	}

	@Override
	public AccumuloSortedMapInterface<K, V> setTimeOutMs(long timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long sizeAsLong() {
		return 0;
	}


	@Override
	public void dump(PrintStream ps) {
	}

	@Override
	public void delete() throws AccumuloException, AccumuloSecurityException,
			TableNotFoundException {
		throw new UnsupportedOperationException();
		
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
		
	}

	@Override
	public Scanner getScanner() throws TableNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AccumuloSortedMapInterface<K, V> sample(double fraction) {
		return this;
	}

	@Override
	public AccumuloSortedMapInterface<K, V> sample(double from_fraction,
			double to_fraction, String randSeed) {
		return this;
	}

	@Override
	public AccumuloSortedMapInterface<K, V> sample(double from_fraction,
			double to_fraction, String randSeed, long max_timestamp) {
		return this;
	}

	@Override
	public boolean isReadOnly() {
		return true;
	}

}
