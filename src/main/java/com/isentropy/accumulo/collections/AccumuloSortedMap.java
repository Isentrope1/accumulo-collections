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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isentropy.accumulo.collections.io.JavaSerializationSerde;
import com.isentropy.accumulo.collections.io.SerDe;
import com.isentropy.accumulo.iterators.AggregateIterator;
import com.isentropy.accumulo.iterators.SamplingFilter;
import com.isentropy.accumulo.iterators.StatsAggregateIterator;
import com.isentropy.accumulo.util.Util;
/**
 * IMPORTANT: 	 
 * In order to use the sorted map features (subMap(), firstKey(),etc), your keySerde must have the property
 * that serialize(a).compareTo(serialize(b)) [byte by byte comparison] = a.compareTo(b) for all objects a and b.
 * If your keySerde does not have this property, the regular map functions (get, put) will still work.
 * Accumulo uses the byte by byte comparator from BytesWritable internally, and I don't know of any way to change the internal comparator. 
 *
 */

public class AccumuloSortedMap<K,V> implements SortedMap<K,V>{

	public static Logger log = LoggerFactory.getLogger(AccumuloSortedMap.class);


	protected static final int ITERATOR_PRIORITY_AGEOFF = 10;
	protected static final String ITERATOR_NAME_AGEOFF = "ageoff";
	private static final int DEFAULT_RANDSEED_LENGTH=20;

	private Connector conn;
	private String table;


	private SerDe keySerde = new JavaSerializationSerde();
	private SerDe valueSerde = new JavaSerializationSerde();
	private byte[] colvis=new byte[0];

	//data
	private byte[] colfam="d".getBytes(StandardCharsets.UTF_8);
	//value
	private byte[] colqual="v".getBytes(StandardCharsets.UTF_8);


	/**
	 * use MockAccumulo, for testing only.
	 * @throws AccumuloSecurityException 
	 * @throws AccumuloException 
	 * @throws TableExistsException 
	 */
	public static AccumuloSortedMap getTestInstance() throws AccumuloException, AccumuloSecurityException {
		Connector c = Util.getMockConnector();
		String table = "asmtest_"+System.currentTimeMillis();
		return new AccumuloSortedMap(c,table);
	}
	public AccumuloSortedMap(Connector c,String table) throws AccumuloException, AccumuloSecurityException {
		conn = c;
		this.table = table;
		init();
	}
	private AccumuloSortedMap(){};
	public SerDe getKeySerde(){
		return keySerde;
	}
	public AccumuloSortedMap<K,V> setKeySerde(SerDe s){
		keySerde=s;
		return this;
	}
	public SerDe getValueSerde(){
		return valueSerde;
	}
	public AccumuloSortedMap<K,V> setValueSerde(SerDe s){
		valueSerde=s;
		return this;
	}
	protected void init() throws AccumuloException, AccumuloSecurityException{
		createTable();
	}

	public String getTable(){
		return table;
	}
	protected Connector getConnector(){
		return conn;
	}

	protected boolean createTable() throws AccumuloException, AccumuloSecurityException{
		try {
			log.info("Creating Accumulo table: "+getTable());
			getConnector().tableOperations().create(getTable());
			return true;
		}
		catch (TableExistsException e) {
			log.debug("table exists: " +getTable());
			return false;
		}
	}


	/**
	 * sets the column family of values
	 * @param cf
	 * @return
	 */
	protected AccumuloSortedMap<K,V> setColumnFamily(byte[] cf){
		colfam = cf;
		return this;
	}
	/**
	 * sets the column qualifier of values
	 * @param cf
	 * @return
	 */
	protected AccumuloSortedMap<K,V> setColumnQualifier(byte[] cq){
		colqual = cq;
		return this;
	}
	/**
	 * sets the column visibility of values
	 * @param cf
	 * @return
	 */
	public AccumuloSortedMap<K,V> setColumnVisibility(byte[] cv){
		colvis = cv;
		return this;
	}
	/**
	 * 
	 * @param timeout the entry timeout in ms. If timeout <= 0, the ageoff feature will be removed
	 * @return
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 * @throws TableNotFoundException
	 */

	public AccumuloSortedMap<K,V> setTimeOutMs(long timeout){
		try{
			EnumSet<IteratorScope> all = EnumSet.allOf(IteratorScope.class);
			getConnector().tableOperations().removeIterator(getTable(), ITERATOR_NAME_AGEOFF, all);
			log.info("Removed timeout for table "+getTable());			
			if(timeout > 0){
				IteratorSetting is = new IteratorSetting(ITERATOR_PRIORITY_AGEOFF,ITERATOR_NAME_AGEOFF,AgeOffFilter.class);
				AgeOffFilter.setNegate(is, false);
				AgeOffFilter.setTTL(is, timeout);
				log.info("Set timeout for table "+getTable()+": "+timeout+" ms");

				try{
					getConnector().tableOperations().attachIterator(getTable(), is, all);
				}
				catch(IllegalArgumentException e){
					log.warn("Attach Iterator threw exception: "+e +"\nThis probably means the iterator was already set.");
				}
			}
			else{
				log.warn("Disabling entry timeout");
				if(getConnector().tableOperations().listIterators(getTable()).containsKey(ITERATOR_NAME_AGEOFF))
					getConnector().tableOperations().removeIterator(getTable(), ITERATOR_NAME_AGEOFF, all);
			}
			return this;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	public long sizeAsLong(){
		return size(null,true,null,true);
	}

	@Override
	public int size(){
		long sz = sizeAsLong();
		if(sz > Integer.MAX_VALUE){
			log.warn("True map size exceeds MAX_INT. Map.size() returning -1. Use sizeAsLong() to get true size.");
			return -1;
		}
		return (int) sz;
	}

	protected long count(Scanner s){
		return MapAggregates.count(s);
	}


	protected long size(final K from,boolean inc1, final K to,boolean inc2){
		Scanner s = null;
		try {
			s = getTableScanner(from,inc1,to,inc2);
			return count(s);
		} catch (TableNotFoundException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	protected Key getKey(Object o){
		if(o == null)
			return null;
		Key k = new Key(getKeySerde().serialize(o),getColumnFamily(),getColumnQualifier(),getColumnVisibility(),System.currentTimeMillis());
		return k;
	}

	public byte[] getColumnFamily(){
		return colfam;
	}
	public byte[] getColumnQualifier(){
		return colqual;
	}
	public byte[] getColumnVisibility(){
		return colvis;
	}

	protected Authorizations getAuthorizations(){
		return new Authorizations(colvis);
	}

	@Override
	public boolean containsKey(Object key) {
		Scanner s = null;
		try {
			s = getScanner();
		} catch (TableNotFoundException e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
		Key k = getKey(key);
		s.setRange(new Range(k,k));
		return s.iterator().hasNext();
	}

	@Override
	public boolean containsValue(Object value) {
		Scanner	s;
		try {
			s = getScanner();
			Iterator<Entry<Key, Value>> it = s.iterator();
			while(it.hasNext()){
				Entry<Key, Value> e = it.next();
				if(Arrays.equals(e.getValue().get(),getValueSerde().serialize(value))){
					return true;
				}
			}
			return false;
		} catch (TableNotFoundException e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Override
	public V get(Object key) {
		Scanner	s;
		try {
			s = getScanner();
		} catch (TableNotFoundException e1) {
			log.error(e1.getMessage());
			throw new RuntimeException(e1);
		}
		Key k1 = getKey(key);
		k1.setTimestamp(0);
		Key k2 = getKey(key);
		k2.setTimestamp(Long.MAX_VALUE);
		s.setRange(new Range(k2,k1));
		Iterator<Entry<Key, Value>> it = s.iterator();
		if(it.hasNext()){
			Entry<Key, Value> e = it.next();
			return (V) getValueSerde().deserialize(e.getValue().get());
		}
		return null;
	}

	protected BatchWriterConfig getBatchWriterConfig(){
		BatchWriterConfig bwc = new BatchWriterConfig();
		bwc.setMaxLatency(300, TimeUnit.SECONDS);
		return bwc;
	}


	/**
	 * dumps key/values to stream. for debugging
	 * @param ps
	 */
	public void dump(PrintStream ps){
		Scanner s;
		try {
			s = getScanner();
			Iterator<Entry<Key,Value>> it = s.iterator();
			while(it.hasNext()){
				Entry<Key,Value> e = it.next();
				ps.println("Key: "+getKeySerde().deserialize(e.getKey().getRowData().toArray()) + "\tValue: "+getValueSerde().deserialize(e.getValue().get()));
			}
		} catch (TableNotFoundException e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
		ps.flush();
	}

	@Override
	public V put(K key, V value) {
		try {
			V prev = this.get(key);
			BatchWriter bw = getConnector().createBatchWriter(getTable(), getBatchWriterConfig());
			Mutation m = new Mutation(getKey(key).getRowData().toArray());

			m.put(getColumnFamily(), getColumnQualifier(),new ColumnVisibility(getColumnVisibility()), getValueSerde().serialize(value));
			bw.addMutation(m);
			bw.flush();
			bw.close();
			//dump(System.out);
			return prev;
		}
		catch(Exception e){
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Override
	public V remove(Object key) {
		try {
			V prev = this.get(key);
			BatchWriter bw = getConnector().createBatchWriter(getTable(), getBatchWriterConfig());
			Mutation m = new Mutation(getKey(key).getRowData().toArray());
			m.putDelete(getColumnFamily(), getColumnQualifier(),new ColumnVisibility(getColumnVisibility()));
			bw.addMutation(m);
			bw.flush();
			return prev;
		}
		catch(Exception e){
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for(Entry<? extends K, ? extends V> e :m.entrySet()){
			put(e.getKey(),e.getValue());
		}
	}
	/**
	 * deletes the map from accumulo!
	 * @throws TableNotFoundException 
	 * @throws AccumuloSecurityException 
	 * @throws AccumuloException 
	 */
	public void delete() throws AccumuloException, AccumuloSecurityException, TableNotFoundException{
		log.warn("Deleting Accumulo table: "+getTable());
		getConnector().tableOperations().delete(getTable());		
	}

	/**
	 * Equivalent to: delete(); createTable();
	 */
	@Override
	public void clear() {
		try {
			delete();
			createTable();
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Override
	public Comparator<? super K> comparator() {
		return new Comparator(){
			@Override
			public int compare(Object o1, Object o2) {
				return getKey(o1).compareTo(getKey(o2));
			}
		};
	}

	/**
	 * null accepted as no limit. returned map is READ ONLY
	 * @param fromKey
	 * @param toKey
	 * @return
	 */
	protected SortedMap<K, V> subMapNullAccepted(final K fromKey,final boolean inc1,final K toKey,final boolean inc2) {
		final AccumuloSortedMap<K, V> parent = this;
		return new AccumuloSortedMap<K,V>(){

			@Override
			public Scanner getScanner() throws TableNotFoundException{
				return parent.getTableScanner(fromKey, inc1, toKey, inc2);
			}

			@Override
			protected Scanner getTableScanner(final K from,final boolean i1,final K to,final boolean i2) throws TableNotFoundException{
				Scanner s = parent.getTableScanner(from,i1,to,i2);
				return s;
			}

			@Override
			protected Authorizations getAuthorizations(){
				return parent.getAuthorizations();
			}
			@Override
			protected Connector getConnector() {
				return parent.getConnector();
			}	

			@Override
			public String getTable(){
				return parent.getTable();
			}
			@Override
			public byte[] getColumnFamily(){
				return parent.getColumnFamily();
			}
			@Override
			public byte[] getColumnQualifier(){
				return parent.getColumnQualifier();
			}
			@Override
			public byte[] getColumnVisibility(){
				return parent.getColumnVisibility();
			}


			@Override
			public long sizeAsLong() {
				return parent.size(fromKey, inc1, toKey, inc2);
			}

			/**
			 * true iff in range
			 * @param k
			 * @return
			 */
			protected boolean rangeCheckTo(Object k){
				Key kk = getKey(k);
				if(toKey != null){
					Key tk = getKey(toKey);
					int cmp= tk.compareTo(kk);
					if(cmp < 0){
						return false;
					}
					else if(!inc2 && cmp == 0){
						return false;
					}
				}
				return true;
			}
			protected boolean rangeCheck(Object k){
				return rangeCheckFrom(k) && rangeCheckTo(k);
			}
			protected boolean rangeCheckFrom(Object k){
				Key kk = getKey(k);
				if(fromKey != null){
					Key fk = getKey(fromKey);
					int cmp= fk.compareTo(kk);
					if(cmp > 0){
						return false;
					}
					else if(!inc1 && cmp == 0){
						return false;
					}
				}
				return true;
			}

			@Override
			public boolean containsValue(Object value) {
				return parent.containsValue(value);
			}

			@Override
			public V get(Object key) {
				if(!rangeCheck(key))
					return null;
				return parent.get(key);
			}

			@Override 
			public AccumuloSortedMap<K, V> setTimeOutMs(long timeout){
				throw new UnsupportedOperationException();
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
			public void clear() {
				throw new UnsupportedOperationException();
			}

			@Override
			public Comparator<? super K> comparator() {
				return parent.comparator();
			}

			@Override
			public SortedMap<K, V> subMap(K fromKeyNew, K toKeyNew) {
				K f = rangeCheckFrom(fromKeyNew) ? fromKeyNew : fromKey;
				K t = rangeCheckTo(toKeyNew) ? toKeyNew : toKey;
				return parent.subMapNullAccepted(f, true, t, false);
			}

			@Override
			public SortedMap<K, V> headMap(K toKeyNew) {
				K t = rangeCheckTo(toKeyNew) ? toKeyNew : toKey;
				return parent.headMap(t);
			}

			@Override
			public SortedMap<K, V> tailMap(K fromKeyNew) {
				K f = rangeCheckFrom(fromKeyNew) ? fromKeyNew : fromKey;
				return parent.tailMap(f);
			}

			@Override
			public K firstKey() {
				return entrySet().iterator().next().getKey();
			}

			@Override
			public K lastKey() {
				throw new UnsupportedOperationException();
			}

			@Override
			public Set<K> keySet() {
				return parent.keySet(fromKey,inc1,toKey,inc1);
			}

			/*
			@Override
			public Collection<V> values() {
				return parent.values(fromKey,true,toKey,false);
			}
			 */

			@Override
			public Set<java.util.Map.Entry<K, V>> entrySet() {
				return new BackingSet(parent);
			}

			@Override
			public SerDe getKeySerde() {
				return parent.getKeySerde();
			}

			@Override
			public SerDe getValueSerde() {
				return parent.getValueSerde();
			}

		};


	}

	@Override
	public SortedMap<K, V> subMap(K fromKey, K toKey) {
		return subMapNullAccepted(fromKey,true,toKey,true);
	}

	@Override
	public SortedMap<K, V> headMap(K toKey) {
		return subMapNullAccepted(null,true,toKey,false);
	}

	@Override
	public SortedMap<K, V> tailMap(K fromKey) {
		return subMapNullAccepted(fromKey,true,null,true);
	}

	@Override
	public K firstKey() {
		return entrySet().iterator().next().getKey();
	}

	@Override
	/**
	 * is there no way in accumulo to efficiently scan to last key??
	 * @return
	 */
	public K lastKey() {
		throw new UnsupportedOperationException();
		/*
		 * this code works but iterates through the entire table:
		 * 
		 * 
		Scanner s;
		try {
			s = getScanner();
			Iterator<Entry<Key,Value>> it = s.iterator();
			K last=null;
			while(it.hasNext()){
				Entry<Key,Value> e = it.next();
				last = (K) serde.deserialize(e.getKey().getRowData().getBackingArray());
			}
			return last;
		} catch (TableNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return null;
		 */
	}

	protected class KeySetIterator implements Iterator<K>{
		Iterator<java.util.Map.Entry<K, V>> wrapped;
		public KeySetIterator(Iterator<java.util.Map.Entry<K, V>> wrapped){
			this.wrapped = wrapped;
		}

		@Override
		public boolean hasNext() {
			return wrapped.hasNext();
		}

		@Override
		public K next() {
			return wrapped.next().getKey();
		}

	}
	protected class ValueSetIterator implements Iterator<V>{
		Iterator<java.util.Map.Entry<K, V>> wrapped;
		public ValueSetIterator(Iterator<java.util.Map.Entry<K, V>> wrapped){
			this.wrapped = wrapped;
		}

		@Override
		public boolean hasNext() {
			return wrapped.hasNext();
		}

		@Override
		public V next() {
			return wrapped.next().getValue();
		}

	}
	/**
	 * set is read only
	 */
	@Override
	public Set<K> keySet() {
		return keySet(null,true,null,true);
	}

	protected Set<K> keySet(final K from,final boolean inc1,final K to,final boolean inc2) {
		final AccumuloSortedMap<K,V> enclosing = this;
		return new Set<K>(){

			@Override
			public int size() {
				return enclosing.size();
			}

			@Override
			public boolean isEmpty() {
				return enclosing.isEmpty();
			}

			@Override
			public boolean contains(Object o) {
				return enclosing.containsKey(o);
			}

			@Override
			public Iterator iterator() {
				return new KeySetIterator(enclosing.iterator());
			}

			@Override
			public Object[] toArray() {
				Object[] o = new Object[size()];
				return toArray(o);
			}

			@Override
			public Object[] toArray(Object[] a) {
				Iterator i = iterator();
				int c=0;
				for(;i.hasNext() && c < a.length;){
					a[c++]=i.next();
				}
				return a;
			}

			@Override
			public boolean add(Object e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean remove(Object o) {
				return false;
			}

			@Override
			public boolean containsAll(Collection c) {
				for(Object o : c){
					if(!contains(o)){
						return false;
					}
				}
				return true;
			}

			@Override
			public boolean addAll(Collection c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean retainAll(Collection c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean removeAll(Collection c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void clear() {
				throw new UnsupportedOperationException();				
			}};
	}

	@Override
	public Collection<V> values() {
		final AccumuloSortedMap<K,V> parent = this;

		return new Collection<V>(){

			@Override
			public int size() {
				long sz = parent.size();
				if(sz > Integer.MAX_VALUE){
					log.warn("True map size exceeds MAX_INT. Map.size() returning -1. Use sizeAsLong() to get true size.");
					return -1;
				}
				return (int) sz;

			}

			@Override
			public boolean isEmpty() {
				return size() == 0;
			}

			@Override
			public boolean contains(Object o) {
				return parent.containsValue(o);
			}

			@Override
			public Iterator<V> iterator() {
				return new ValueSetIterator(parent.iterator());
			}

			@Override
			public Object[] toArray() {
				Object[] o = new Object[size()];
				return toArray(o);
			}

			@Override
			public Object[] toArray(Object[] a) {
				Iterator i = iterator();
				int c=0;
				for(;i.hasNext() && c < a.length;){
					a[c++]=i.next();
				}
				return a;
			}

			@Override
			public boolean add(V e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean remove(Object o) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean containsAll(Collection<?> c) {
				for(Object o:c){
					if(!contains(o))
						return false;
				}
				return true;
			}

			@Override
			public boolean addAll(Collection<? extends V> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean removeAll(Collection<?> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean retainAll(Collection<?> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void clear() {
				throw new UnsupportedOperationException();
			}
		};
	}

	/**
	 * 
	 * @param from
	 * @param inc1
	 * @param to
	 * @param inc2
	 * @return a Scanner over all entries of the base table in the specified range
	 * @throws TableNotFoundException
	 */
	protected Scanner getTableScanner(final K from,final boolean inc1,final K to,final boolean inc2) throws TableNotFoundException{
		Scanner s = getConnector().createScanner(getTable(), getAuthorizations());
		s.setRange(new Range(getKey(from),inc1,getKey(to),inc2));
		return s;
	}

	/**
	 * 
	 * @return a Scanner over all rows visible to this map
	 * @throws TableNotFoundException
	 */
	public Scanner getScanner() throws TableNotFoundException{
		return getTableScanner(null,true,null,true);
	}

	public AccumuloSortedMap<K,V> sample(final double fraction){
		return sample(0,fraction,Util.randomHexString(DEFAULT_RANDSEED_LENGTH),-1);
	}

	public AccumuloSortedMap<K,V> sample(final double from_fraction, final double to_fraction,final String randSeed){
		return sample(from_fraction,to_fraction,randSeed, -1);
	}	

	public AccumuloSortedMap<K,V> sample(final double from_fraction, final double to_fraction,final String randSeed, final long max_timestamp){
		final AccumuloSortedMap<K,V> parent = this;

		AccumuloSortedMap<K,V> sampled = new AccumuloSortedMap<K,V>(){

			@Override
			public Scanner getScanner() throws TableNotFoundException{
				Scanner s = parent.getScanner();
				IteratorSetting cfg = new IteratorSetting(Integer.MAX_VALUE, SamplingFilter.class);
				cfg.addOption(SamplingFilter.OPT_FROMFRACTION, Double.toString(from_fraction));
				cfg.addOption(SamplingFilter.OPT_TOFRACTION, Double.toString(to_fraction));
				cfg.addOption(SamplingFilter.OPT_RANDOMSEED, randSeed);
				if(max_timestamp > 0)
					cfg.addOption(SamplingFilter.OPT_MAXTIMESTAMP, Long.toString(max_timestamp));
				s.addScanIterator(cfg);
				return s;
			}

			@Override 
			public AccumuloSortedMap<K, V> setTimeOutMs(long timeout){
				throw new UnsupportedOperationException();
			}

			@Override
			protected Scanner getTableScanner(final K from,final boolean inc1,final K to,final boolean inc2) throws TableNotFoundException{
				Scanner s = parent.getTableScanner(from,inc1,to,inc2);
				return s;
			}
			@Override
			public SerDe getKeySerde() {
				return parent.getKeySerde();
			}
			@Override
			public SerDe getValueSerde() {
				return parent.getValueSerde();
			}
			@Override
			protected Authorizations getAuthorizations(){
				return parent.getAuthorizations();
			}
			@Override
			protected Connector getConnector() {
				return parent.getConnector();
			}	

			@Override
			public String getTable(){
				return parent.getTable();
			}
			@Override
			public byte[] getColumnFamily(){
				return parent.getColumnFamily();
			}
			@Override
			public byte[] getColumnQualifier(){
				return parent.getColumnQualifier();
			}
			@Override
			public byte[] getColumnVisibility(){
				return parent.getColumnVisibility();
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
			public void clear() {
				throw new UnsupportedOperationException();
			}
			@Override
			public void delete(){
				throw new UnsupportedOperationException();
			}


		};
		return sampled;
	}

	protected Iterator<java.util.Map.Entry<K, V>> iterator(){
		return new Iterator<java.util.Map.Entry<K, V>>(){
			Scanner s;
			Iterator<Entry<Key, Value>> wrapped;
			//boolean initFailed = false;
			void initScanner(){
				if(s!=null)
					return;
				try{
					s = getScanner();
					wrapped=s.iterator();
				}
				catch(TableNotFoundException e){
					log.error(e.getMessage());
					throw new RuntimeException(e);
				}
			}
			@Override
			public boolean hasNext() {
				initScanner();
				return wrapped.hasNext();
			}

			@Override
			public java.util.Map.Entry<K, V> next() throws NoSuchElementException{
				if(!hasNext()){
					throw new NoSuchElementException();
				}
				final Entry<Key,Value> n = wrapped.next();
				return new Map.Entry<K, V>(){
					V val = (V) getValueSerde().deserialize(n.getValue().get());

					@Override
					public K getKey() {
						return (K) getKeySerde().deserialize(n.getKey().getRowData().toArray());
					}

					@Override
					public V getValue() {
						return val;
					}

					@Override
					public V setValue(V value) {
						V prev = val;
						val = value;
						return prev;
					}
				}; 
			}
		};
	}

	/**
	 * read only, iterator returns keys in sorted order
	 *
	 */
	protected class BackingSet implements Set<java.util.Map.Entry<K, V>>{
		final AccumuloSortedMap<K,V> parent;
		//K from,to;
		//boolean inc1,inc2;
		protected BackingSet(AccumuloSortedMap<K,V> p){
			parent = p;
		}

		@Override
		public int size() {
			return parent.size();
		}

		@Override
		public boolean isEmpty() {
			return parent.isEmpty();
		}

		@Override
		public boolean contains(Object o) {
			if(!(o instanceof Map.Entry)){
				return false;
			}
			Map.Entry e = (Map.Entry) o;
			return parent.containsKey(e.getKey());
		}
		@Override
		public Iterator<java.util.Map.Entry<K, V>> iterator() {
			return parent.iterator();
		}


		@Override
		public Object[] toArray() {
			Object[] o = new Object[size()];
			return toArray(o);
		}

		@Override
		public Object[] toArray(Object[] a) {
			Iterator i = iterator();
			int c=0;
			for(;i.hasNext() && c < a.length;){
				a[c++]=i.next();
			}
			return a;
		}

		@Override
		public boolean add(java.util.Map.Entry<K, V> e) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean remove(Object o) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			for(Object o:c){
				if(!contains(o))
					return false;
			}
			return true;
		}

		@Override
		public boolean addAll(
				Collection<? extends java.util.Map.Entry<K, V>> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException();
		}
	}; 

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		final AccumuloSortedMap<K,V> parent = this;
		return new BackingSet(parent);
	}

}
