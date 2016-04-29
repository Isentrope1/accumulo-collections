/* 
Accumulo Collections
Copyright 2016 Isentropy LLC
Written by Jonathan Wolff <jwolff@isentropy.com>
Isentropy specializes in big data and quantitative programming consulting,
with particular expertise in Accumulo development and installation. 
More info at http://isentropy.com.


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
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
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.isentropy.accumulo.collections.io.JavaSerializationSerde;
import com.isentropy.accumulo.collections.io.SerDe;

public class EmptyAccumuloSortedMap<K,V> extends AccumuloSortedMapBase<K, V> {

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

	protected AccumuloSortedMapBase<K, V> subMap(final K fromKey,final boolean inc1,final K toKey,final boolean inc2){
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
	public AccumuloSortedMapBase<K, V> setKeySerde(SerDe s) {
		return this;
	}

	@Override
	public SerDe getValueSerde() {
		return new JavaSerializationSerde();
	}

	@Override
	public AccumuloSortedMapBase<K, V> setValueSerde(SerDe s) {
		return this;
	}

	@Override
	public String getTable() {
		return null;
	}

	@Override
	public AccumuloSortedMapBase<K, V> setColumnVisibility(byte[] cv) {
		return this;
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
	public boolean isReadOnly() {
		return true;
	}

	@Override
	protected AccumuloSortedMapBase<K,V> derivedMapFromIterator(Class<? extends SortedKeyValueIterator<Key, Value>> iterator, Map<String,String> iterator_options, SerDe derivedMapValueSerde){
		return this;
	}

	@Override
	public AccumuloSortedMapBase<K, V> sample(double from_fraction,
			double to_fraction, String randSeed, long max_timestamp) {
			return this;
	}
	
	@Override
	public long getTimestamp(K key){
		return -1;
	}

}
