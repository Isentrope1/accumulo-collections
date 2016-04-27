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
import java.util.HashMap;
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
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
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

public class AccumuloSortedMap<K,V> extends  AccumuloSortedMapBase<K, V>{

	public static Logger log = LoggerFactory.getLogger(AccumuloSortedMap.class);


	protected static final int ITERATOR_PRIORITY_AGEOFF = 10;
	protected static final int SUBMAP_ITERATOR_PRIORITY_MIN = 100;
	protected static final String ITERATOR_NAME_AGEOFF = "ageoff";

	private Connector conn;
	private String table;


	private SerDe keySerde = new JavaSerializationSerde();
	private SerDe valueSerde = new JavaSerializationSerde();
	private byte[] colvis=new byte[0];

	//data
	private byte[] colfam="d".getBytes(StandardCharsets.UTF_8);
	//value
	private byte[] colqual="v".getBytes(StandardCharsets.UTF_8);

	//submaps may pile on iterators to chain of getScanner. this int is the next iterator priority

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
	protected AccumuloSortedMap(){};
	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#getKeySerde()
	 */
	@Override
	public SerDe getKeySerde(){
		return keySerde;
	}
	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#setKeySerde(com.isentropy.accumulo.collections.io.SerDe)
	 */
	@Override
	public AccumuloSortedMapBase<K, V> setKeySerde(SerDe s){
		keySerde=s;
		return this;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#getValueSerde()
	 */
	@Override
	public SerDe getValueSerde(){
		return valueSerde;
	}
	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#setValueSerde(com.isentropy.accumulo.collections.io.SerDe)
	 */
	@Override
	public AccumuloSortedMapBase<K, V> setValueSerde(SerDe s){
		valueSerde=s;
		return this;
	}
	protected void init() throws AccumuloException, AccumuloSecurityException{
		createTable();
	}

	/**
	 * submaps may pile on iterators to chain of getScanner. this method returns the next iterator priority
	 */
	protected int nextIteratorPriority(){
		return SUBMAP_ITERATOR_PRIORITY_MIN;
	}


	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#getTable()
	 */
	@Override
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
	protected AccumuloSortedMapBase<K, V> setColumnFamily(byte[] cf){
		colfam = cf;
		return this;
	}
	/**
	 * sets the column qualifier of values
	 * @param cf
	 * @return
	 */
	protected AccumuloSortedMapBase<K, V> setColumnQualifier(byte[] cq){
		colqual = cq;
		return this;
	}
	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#setColumnVisibility(byte[])
	 */
	@Override
	public AccumuloSortedMapBase<K, V> setColumnVisibility(byte[] cv){
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
	public AccumuloSortedMapBase<K, V> setTimeOutMs(long timeout){
		if(isReadOnly())
			throw new UnsupportedOperationException();
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

	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#sizeAsLong()
	 */
	@Override
	public long sizeAsLong(){
		return MapAggregates.count(this);
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


	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#dump(java.io.PrintStream)
	 */
	@Override
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
		if(isReadOnly())
			throw new UnsupportedOperationException();
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
		if(isReadOnly())
			throw new UnsupportedOperationException();
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
		if(isReadOnly())
			throw new UnsupportedOperationException();

		for(Entry<? extends K, ? extends V> e :m.entrySet()){
			put(e.getKey(),e.getValue());
		}
	}
	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#delete()
	 */
	@Override
	public void delete() throws AccumuloException, AccumuloSecurityException, TableNotFoundException{
		if(isReadOnly())
			throw new UnsupportedOperationException();

		log.warn("Deleting Accumulo table: "+getTable());
		getConnector().tableOperations().delete(getTable());		
	}

	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#clear()
	 */
	@Override
	public void clear() {
		if(isReadOnly())
			throw new UnsupportedOperationException();

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
	 * always returns a AccumuloSortedMapInterface
	 * null accepted as no limit. returned map is READ ONLY
	 * @param fromKey
	 * @param toKey
	 * @return
	 */
	protected AccumuloSortedMapBase<K, V> subMapNullAccepted(final K fromKey,final boolean inc1,final K toKey,final boolean inc2) {
		final AccumuloSortedMap<K, V> parent = this;
		Scanner s;
		try {
			s = parent.getScanner();
		} catch (TableNotFoundException e) {
			throw new RuntimeException(e);
		}

		Range curRange = s.getRange();
		Range requestedRange = new Range(getKey(fromKey),inc1,getKey(toKey),inc2);
		final Range newRange = curRange == null ? requestedRange : curRange.clip(requestedRange,true);
		//ranges are disjoint
		if(newRange == null)
			return new EmptyAccumuloSortedMap();

		return new AccumuloSortedMap<K,V>(){
			@Override 
			protected int nextIteratorPriority(){
				//subMap() doesnt add an iterator
				return parent.nextIteratorPriority();
			}
			@Override
			public boolean isReadOnly() {
				return true;
			}
			@Override
			public Scanner getScanner() throws TableNotFoundException{
				Scanner s = parent.getScanner();
				s.setRange(newRange);
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
			public SerDe getKeySerde() {
				return parent.getKeySerde();
			}

			@Override
			public SerDe getValueSerde() {
				return parent.getValueSerde();
			}
			@Override
			public Comparator<? super K> comparator() {
				return parent.comparator();
			}

			@Override
			public V get(Object key) {
				if(!newRange.contains(getKey(key)))
					return null;
				return parent.get(key);
			}
			//AccumuloSortedMap write operations throw exception if isReadOnly()
		};


	}

	/**
	 * returns an instance of AccumuloSortedMapInterface
	 */
	@Override
	public AccumuloSortedMapBase<K, V> subMap(K fromKey, K toKey) {
		return subMapNullAccepted(fromKey,true,toKey,true);
	}

	@Override
	public AccumuloSortedMapBase<K, V> headMap(K toKey) {
		return subMapNullAccepted(null,true,toKey,false);
	}

	@Override
	public AccumuloSortedMapBase<K, V> tailMap(K fromKey) {
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

	protected Scanner getScanner() throws TableNotFoundException{
		Scanner s = getConnector().createScanner(getTable(), getAuthorizations());
		return s;
	}


	@Override
	public AccumuloSortedMapBase<K,V> derivedMapFromIterator(Class<? extends SortedKeyValueIterator<Key, Value>> iterator, Map<String,String> iterator_options, SerDe derivedMapValueSerde){
		Map<String,String> itcfg = new HashMap<String,String>();
		if(iterator_options != null)
			itcfg.putAll(iterator_options);
		return new IteratorStackedSubmap<K,V>(this,iterator,itcfg,derivedMapValueSerde);		
	}

	public AccumuloSortedMapBase<K, V> sample(final double from_fraction, final double to_fraction,final String randSeed, final long max_timestamp){
		Map<String,String> cfg = new HashMap<String,String>();
		cfg.put(SamplingFilter.OPT_FROMFRACTION, Double.toString(from_fraction));
		cfg.put(SamplingFilter.OPT_TOFRACTION, Double.toString(to_fraction));
		cfg.put(SamplingFilter.OPT_RANDOMSEED, randSeed);
		if(max_timestamp > 0)
			cfg.put(SamplingFilter.OPT_MAXTIMESTAMP, Long.toString(max_timestamp));
		return derivedMapFromIterator(SamplingFilter.class,cfg,getValueSerde());
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
