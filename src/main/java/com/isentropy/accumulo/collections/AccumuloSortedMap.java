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

import static com.isentropy.accumulo.collections.ForeignKey.resolve;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
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
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isentropy.accumulo.collections.io.FixedPointSerde;
import com.isentropy.accumulo.collections.io.JavaSerializationSerde;
import com.isentropy.accumulo.collections.io.SerDe;
import com.isentropy.accumulo.collections.mappers.CountsDerivedMapper;
import com.isentropy.accumulo.collections.mappers.RowStatsMapper;
import com.isentropy.accumulo.collections.transform.KeyValueTransformer;
import com.isentropy.accumulo.iterators.AggregateIterator;
import com.isentropy.accumulo.iterators.KeyToKeyMapTransformer;
import com.isentropy.accumulo.iterators.RegexFilter;
import com.isentropy.accumulo.iterators.SamplingFilter;
import com.isentropy.accumulo.iterators.StatsAggregateIterator;
import com.isentropy.accumulo.util.KeyValue;
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

	private static final int DEFAULT_RANDSEED_LENGTH=20;
	public static long DEFAULT_WAIT_MS = 1000;
	// this is the priority that the first stacked iterator will be be added at
	protected static final int DERIVEDMAP_ITERATOR_PRIORITY_MIN = 100;
	protected static final String ITERATOR_NAME_AGEOFF = "ageoff";
	//"vers" is the default VersioningIterator name in Accumulo
	protected static final String ITERATOR_NAME_VERSIONING = "vers";

	protected static final int ITERATOR_PRIORITY_AGEOFF = 10;


	protected static final int ITERATOR_PRIORITY_VERSIONING = 20;
	public static Logger log = LoggerFactory.getLogger(AccumuloSortedMap.class);
	/*
	 * iterators used in deriveMap will be passed SerDe classname info via these iterator params
	 */
	public static final String OPT_KEY_SERDE = "key_serde";

	public static final String OPT_VALUE_INPUT_SERDE = "value_input_serde";
	public static final String OPT_VALUE_OUTPUT_SERDE = "value_output_serde";
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
		public boolean add(java.util.Map.Entry<K, V> e) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean addAll(
				Collection<? extends java.util.Map.Entry<K, V>> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException();
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
		public boolean containsAll(Collection<?> c) {
			for(Object o:c){
				if(!contains(o))
					return false;
			}
			return true;
		}

		@Override
		public boolean isEmpty() {
			return parent.isEmpty();
		}

		@Override
		public Iterator<java.util.Map.Entry<K, V>> iterator() {
			return parent.iterator();
		}

		@Override
		public boolean remove(Object o) {
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
		public int size() {
			return parent.size();
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
	}
	protected class EntrySetIterator implements Iterator<java.util.Map.Entry<K, V>>{
		Iterator<Entry<Key, Value>> wrapped;
		protected EntrySetIterator(Iterator<Entry<Key, Value>> source){
			wrapped = source;
		}
		@Override
		public boolean hasNext() {
			return wrapped.hasNext();
		}

		@Override
		public java.util.Map.Entry<K, V> next() {
			if(!hasNext()){
				throw new NoSuchElementException();
			}
			final Entry<Key,Value> n = wrapped.next();
			return new Map.Entry<K, V>(){
				V val = deserializeValue(n.getValue().get());

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
	protected class Submap extends ScannerDerivedMap<K,V>{
		private Range range;
		public Submap(Range r, AccumuloSortedMap<K, V> parent, SerDe derivedMapValueSerde) {
			super(parent, derivedMapValueSerde);
			range = r;
		}

		@Override
		public V get(Object key) {
			if(!range.contains(getKey(key)))
				return null;
			return (V) parent.get(key);
		}
		
		@Override
		public Iterator<V> getAll(Object key) {
			if(!range.contains(getKey(key)))
				return null;
			return (Iterator<V>) parent.getAll(key);
		}
		
		@Override
		public Scanner getMultiScanner() throws TableNotFoundException {
			Scanner s = parent.getMultiScanner();
			s.setRange(range);
			return s;
		}
		@Override
		public Scanner getScanner() throws TableNotFoundException {
			Scanner s = parent.getScanner();
			s.setRange(range);
			return s;
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
	protected BatchWriter batchWriter;
	private BatchWriterConfig batchWriterConfig = getDefaultBatchWriterConfig();
	//data
	private byte[] colfam="d".getBytes(StandardCharsets.UTF_8);
	//value
	private byte[] colqual="v".getBytes(StandardCharsets.UTF_8);
	private byte[] colvis=new byte[0];

	//submaps may pile on iterators to chain of getScanner. this int is the next iterator priority

	private Connector conn;
	private SerDe keySerde = new FixedPointSerde();
	private boolean readOnly = false;
	
	/**
	 *  clearable is flag that permits clear() and delete(), which wipe the entire table.
	 *  It is false by default.
	 */
	private boolean clearable = false;
	
	
	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#getKeySerde()
	 */

	private String table;
	private SerDe valueSerde = new FixedPointSerde();

	protected AccumuloSortedMap(){}

	/**
	 * Use AccumuloSortedMapFactory.makeMap() instead
	 * @param c
	 * @param table
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	
	@Deprecated
	public AccumuloSortedMap(Connector c,String table) throws AccumuloException, AccumuloSecurityException {
		this(c,table,true,false);
	}

	/**
	 * 	 * Use AccumuloSortedMapFactory.makeMap() instead
	 * @param c
	 * @param table
	 * @param createTable
	 * @param errorIfTableAlreadyExists
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	@Deprecated
	public AccumuloSortedMap(Connector c,String table, boolean createTable,boolean errorIfTableAlreadyExists) throws AccumuloException, AccumuloSecurityException {
		conn = c;
		this.table = table;
		init(createTable,errorIfTableAlreadyExists);
	}
	
	protected void addPutMutation(K key, V value, BatchWriter bw) throws MutationsRejectedException {
		Mutation m = new Mutation(getKey(key).getRowData().toArray());
		m.put(getColumnFamily(), getColumnQualifier(),new ColumnVisibility(getColumnVisibility()), getValueSerde().serialize(value));
		bw.addMutation(m);		
	}

	
	protected void addRemoveMutation(Object key, BatchWriter bw) throws MutationsRejectedException{
		Mutation m = new Mutation(getKey(key).getRowData().toArray());
		m.putDelete(getColumnFamily(), getColumnQualifier(),new ColumnVisibility(getColumnVisibility()));
		bw.addMutation(m);
	}

	
	/**
	 * adds delete mutation to BatchWriter, but doesn't flush it.
	 * use flushCachedEdits() flush.
	 * @param key
	 * @throws MutationsRejectedException
	 */
	public void cacheRemove(K key) throws MutationsRejectedException {
		if(isReadOnly())
			throw new UnsupportedOperationException();
		addRemoveMutation(key, getBatchWriter());
	}
	/**
	 * adds put mutation to BatchWriter, but doesn't flush it.
	 * use flushCachedEdits() flush.
	 * @param key
	 * @throws MutationsRejectedException
	 */
	public void cachePut(K key, V value) throws MutationsRejectedException {
		if(isReadOnly())
			throw new UnsupportedOperationException();
		addPutMutation(key,value, getBatchWriter());
	}

	/**
	 * a full map checksum to ensure data integrity
	 * computed on tablet servers using MapChecksumAggregateIterator
	 * 
	 * @return a long whose top int is the sum of keys' hashCode() and 
	 *  whose bottom int is the sum of values' hashCode(). Note that this
	 *  checksum runs on the deserialized java objects and should therefore be
	 *  independent of SerDe
	 * @throws MutationsRejectedException 
	 */		
	
	public final long checksum(){
		return checksum(false);
	}
	public final int checksumKeys(){
		return checksumKeys(false);
	}

	public final int checksumValues(){
		return checksumValues(false);
	}
	public long checksum(boolean includeMultipleValues){
		return MapAggregates.checksum(this,includeMultipleValues);
	}
	public final int checksumKeys(boolean includeMultipleValues){
		return (int) (checksum(includeMultipleValues) >>> 32);
	}

	public final int checksumValues(boolean includeMultipleValues){
		return (int) checksum(includeMultipleValues);
	}

	@Override
	public void clear() {
		if(isReadOnly())
			throw new UnsupportedOperationException("cant clear() readOnly map");
		if(!isClearable())
			throw new UnsupportedOperationException("must set setClearable(true) before calling clear()");
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

	protected Map<String,String> configureSerdes(Map<String,String> opts,SerDe output_value_serde){
		if(opts == null)
			opts = new HashMap<String,String>();
		opts.put(OPT_KEY_SERDE, getKeySerde().getClass().getName());
		opts.put(OPT_VALUE_INPUT_SERDE, getValueSerde().getClass().getName());
		if(output_value_serde != null)
			opts.put(OPT_VALUE_OUTPUT_SERDE, output_value_serde.getClass().getName());
		else
			opts.put(OPT_VALUE_OUTPUT_SERDE, getValueSerde().getClass().getName());
		return opts;
	}

	@Override
	public boolean containsKey(Object key) {
		return get(key) != null;
	}
	/**
	 * runs through the entire table locally. slow.
	 * TODO maybe add iterator for this
	 */
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
	protected boolean createTable() throws AccumuloException, AccumuloSecurityException, TableExistsException{
		log.info("Creating Accumulo table: "+getTable());
		getConnector().tableOperations().create(getTable());
		return true;
	}

	public void delete() throws AccumuloException, AccumuloSecurityException, TableNotFoundException{
		if(isReadOnly())
			throw new UnsupportedOperationException();
		if(!isClearable())
			throw new UnsupportedOperationException("must set setClearable(true) before calling delete()");
		if(batchWriter != null)
			batchWriter.close();
		batchWriter = null;
		log.warn("Deleting Accumulo table: "+getTable());
		getConnector().tableOperations().delete(getTable());		
	}

	public IteratorStackedSubmap<K,?> derivedMapFromIterator(Class<? extends SortedKeyValueIterator<Key, Value>> iterator, Map<String,String> iterator_options, SerDe derivedMapValueSerde, boolean isAggregate){
		Map<String,String> itcfg = new HashMap<String,String>();
		if(iterator_options != null)
			itcfg.putAll(iterator_options);
		IteratorStackedSubmap<K,?> iss =  new IteratorStackedSubmap<K,V>(this,iterator,itcfg,derivedMapValueSerde);
		iss.setAggregate(isAggregate);
		return iss;
	}

	/**
	 * Create a derived map by stacking an iterator and options specified by a DerivedMapper.
	 * 
	 * wraps derivedMapFromIterator and add iterator options OPT_KEY_SERDE, OPT_VALUE_INPUT_SERDE, OPT_VALUE_OUTPUT_SERDE
	 * that specify the classname of the key and value serdes used by this map

	 * if mapper.getDerivedMapValueSerde() == null, OPT_VALUE_OUTPUT_SERDE will be set to same value as OPT_VALUE_INPUT_SERDE
   and derivedMapFromIterator will be passed the current map's value serde
	 * @param mapper
	 * @return
	 */
	public AccumuloSortedMap<K,?> deriveMap(DerivedMapper mapper){
		return deriveMap(mapper,false);
	}




	public AccumuloSortedMap<K,?> deriveMap(DerivedMapper mapper, boolean isRowAggregate){
		SerDe output_value_serde = mapper.getDerivedMapValueSerde();
		Map<String,String> opts = configureSerdes(mapper.getIteratorOptions(),output_value_serde);
		return derivedMapFromIterator(mapper.getIterator(),opts, output_value_serde == null ? getValueSerde():output_value_serde,isRowAggregate);
	}

	protected V deserializeValue(byte[] b){
		V o = (V) getValueSerde().deserialize(b);
		if(o != null && o instanceof ForeignKey){
			((ForeignKey) o).setConnector(getConnector());
		}
		return o;
	}

	/**
	 * dumps key/values to stream. for debugging
	 * @param ps
	 */
	public void dump(PrintStream ps){
		dump(ps,-1);
	}
	public void dump(PrintStream ps,int max_values_per_key){
		K prev=null;
		int valuesShown=0;
		for(Iterator<Entry<K,V>> it =  multiEntryIterator();it.hasNext();){
			Entry<K,V> e = it.next();
			K k = e.getKey();
			if(prev == null || !k.equals(prev) ){
				valuesShown=0;
			}
			if(max_values_per_key < 0 || valuesShown++ < max_values_per_key){
				ps.println(formatDumpLine(e));				
			}
		}
		ps.flush();
	}
	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		final AccumuloSortedMap<K,V> parent = this;
		return new BackingSet(parent);
	}
	
	@Override
	public K firstKey() {
		return entrySet().iterator().next().getKey();
	}
	
	public void flushCachedEdits() throws MutationsRejectedException{
		BatchWriter bw = getBatchWriter();
		if(bw != null)
			bw.flush();
	}

	protected String formatDumpLine(Map.Entry<K,V> e){
		return "k = "+e.getKey()+" : v = "+e.getValue();
	}
	
	@Override
	public V get(Object key) {
		Entry<Key, Value> e = getEntry(key);
		if(e == null)
			return null;
		return deserializeValue(e.getValue().get());		
	}


	/**
	 * returns all values associated with key. Should be <=n values when setMultiMap(n) is enabled
	 * @param key
	 * @return
	 */
	public Iterator<V> getAll(Object key){
		Scanner	s;
		try {
			// get scanner without versioning iterator
			s = getMultiScanner();
			Key k1 = getKey(key);
			k1.setTimestamp(0);
			Key k2 = getKey(key);
			k2.setTimestamp(Long.MAX_VALUE);
			s.setRange(new Range(k2,k1));
			Iterator<Entry<Key, Value>> it = s.iterator();
			return new ValueSetIterator(new EntrySetIterator(it));
		} catch (TableNotFoundException e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}


	protected Authorizations getAuthorizations(){
		return new Authorizations(colvis);
	}


	protected BatchWriter getBatchWriter(){
		if(batchWriter == null){
			try {
				reinitBatchWriter();
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new RuntimeException(e);
			}
		}
		return batchWriter;
	}


	public BatchWriterConfig getBatchWriterConfig() {
		return batchWriterConfig;
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


	protected Connector getConnector(){
		return conn;
	}

	protected BatchWriterConfig getDefaultBatchWriterConfig(){
		BatchWriterConfig bwc = new BatchWriterConfig();
		bwc.setMaxLatency(300, TimeUnit.SECONDS);
		return bwc;
	}
	protected Entry<Key, Value> getEntry(Object key) {
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
			return e;
		}
		return null;
	}

	/**
	 * translates Object key to and Accumulo Key
	 * @param o
	 * @return
	 */
	
	protected Key getKey(Object key){
		if(key == null)
			return null;
		Key k = new Key(getKeySerde().serialize(key),getColumnFamily(),getColumnQualifier(),getColumnVisibility(),System.currentTimeMillis());
		return k;
	}

	public SerDe getKeySerde(){
		return keySerde;
	}
	
	private static final String MAXVERSIONS_OPT = "maxVersions";
	public int getMaxValuesPerKey() throws AccumuloSecurityException, AccumuloException, TableNotFoundException{
		if(getConnector().tableOperations().listIterators(getTable()).containsKey(ITERATOR_NAME_VERSIONING)){
			IteratorSetting is = getConnector().tableOperations().getIteratorSetting(getTable(), ITERATOR_NAME_VERSIONING, IteratorScope.majc);
			String maxversions = is.getOptions().get(MAXVERSIONS_OPT);
			if(maxversions != null)
				return Integer.parseInt(maxversions);
		}
		return -1;
	}

	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#delete()
	 */

	protected Scanner getMultiScanner() throws TableNotFoundException{
		Scanner s = getConnector().createScanner(getTable(), getAuthorizations());
		return s;
	}


	/**
	 * getScanner() always sees 1 value per key. 
	 * getMultiScanner() sees multiple when setMultiMap(n) has been called with n > 1 
	 */
	protected Scanner getScanner() throws TableNotFoundException{
		Scanner s = getConnector().createScanner(getTable(), getAuthorizations());
		EnumSet<IteratorScope> all = EnumSet.allOf(IteratorScope.class);
		int prior = ITERATOR_PRIORITY_VERSIONING;
		IteratorSetting is = new IteratorSetting(prior,ITERATOR_NAME_VERSIONING+prior,VersioningIterator.class);
		VersioningIterator.setMaxVersions(is, 1);
		s.addScanIterator(is);
		return s;
	}

	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#getTable()
	 */
	public String getTable(){
		return table;
	}

	private static final String TTL = "ttl";
	public long getTimeOutMs() throws NumberFormatException, AccumuloSecurityException, AccumuloException, TableNotFoundException{
		if(getConnector().tableOperations().listIterators(getTable()).containsKey(ITERATOR_NAME_AGEOFF)){
			IteratorSetting is = getConnector().tableOperations().getIteratorSetting(getTable(), ITERATOR_NAME_AGEOFF, IteratorScope.majc);
			String ttl = is.getOptions().get(TTL);
			if(ttl != null)
				return Long.parseLong(ttl);
		}
		return -1;
	}
	
	public long getTimestamp(K key){
		Entry<Key, Value> e = getEntry(key);
		if(e == null)
			return -1;
		return e.getKey().getTimestamp();		
	}

	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#getValueSerde()
	 */
	public SerDe getValueSerde(){
		return valueSerde;
	}

	@Override
	public final AccumuloSortedMap<K, V> headMap(K toKey) {
		return subMap(null,true,toKey,false);
	}

	public long importAll(Iterator it) {
		return importAll(it,null,true);
	}
	/**
	 * for bulk import. unlike put(), does not flush BatchWriter after each entry
	 * 
	 * @param it an iterator of type Iterator<Map.Entry<? extends K, ? extends V>>
	 * 
	 * @return a long checksum containing (sum of keys' hashCode(), sum of values' hashCode()) 
	 *  or 0 if computeChecksum is false
	 */
	public long importAll(Iterator it,KeyValueTransformer trans,boolean computeChecksum) {
		if(isReadOnly())
			throw new UnsupportedOperationException();

		long keySum=0;
		int valueSum=0;
		try{
			BatchWriter bw = getBatchWriter();
			for(;it.hasNext();){
				Entry e = (Entry) it.next();
				K key = (K) e.getKey();
				V value = (V) e.getValue();
				if(trans != null){
					Entry tranformed = trans.transformKeyValue(key, value);
					addPutMutation((K) tranformed.getKey(), (V) tranformed.getValue(), bw);										
				}
				else{
					addPutMutation(key,value,bw);					
				}
				if(computeChecksum){
					keySum += key.hashCode();
					valueSum += value.hashCode();
				}
			}
			bw.flush();
			return (keySum << 32) | valueSum;
		}
		catch(MutationsRejectedException e){
			log.error(e.getMessage());
			try {
				reinitBatchWriter();
				throw new RuntimeException(e);
			} catch (Exception e1) {
				log.error(e1.getMessage());
				throw new RuntimeException(e1);
			}
		}
	}
	protected void init(boolean createTable, boolean errorIfTableAlreadyExists) throws AccumuloException, AccumuloSecurityException{
		try {
			if(createTable){
				if(!conn.tableOperations().list().contains(getTable()))
					createTable();
				else if(errorIfTableAlreadyExists)
					throw new AccumuloException("tried to create existing table "+getTable()+" and errorIfTableAlreadyExists=true");
			}
		}
		catch (TableExistsException e) {
			if(errorIfTableAlreadyExists)
				throw new AccumuloException(e);
			else{
				log.info("table already exists: "+getTable());
			}
		}
	}
	public boolean isClearable() {
		return clearable;
	}

	public boolean isEmpty() {
		return size() == 0;
	}


	public boolean isReadOnly(){
		return readOnly;
	}

	protected Iterator<java.util.Map.Entry<K, V>> iterator(){
		try {
			return new EntrySetIterator(getScanner().iterator());
		} catch (TableNotFoundException e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
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
			public boolean add(Object e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean addAll(Collection c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void clear() {
				throw new UnsupportedOperationException();				
			}

			@Override
			public boolean contains(Object o) {
				return enclosing.containsKey(o);
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
			public boolean isEmpty() {
				return enclosing.isEmpty();
			}

			@Override
			public Iterator iterator() {
				return new KeySetIterator(enclosing.iterator());
			}

			@Override
			public boolean remove(Object o) {
				return false;
			}

			@Override
			public boolean removeAll(Collection c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean retainAll(Collection c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public int size() {
				return enclosing.size();
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
			}};
	}
	
	public StatisticalSummary keyStats(){
		return keyToKeyMap().valueStats(false);
	}
	
	public AccumuloSortedMap<K, K> keyToKeyMap() {
		Map<String,String> cfg = new HashMap<String,String>();
		configureSerdes(cfg,getKeySerde());
		return new IteratorStackedSubmap<K,K>(this,KeyToKeyMapTransformer.class,cfg,getKeySerde());
	}
	

	/**
	 * is there no way in accumulo to efficiently scan to last key??
	 * 
	 * this method uses count iterator (which returns last key from each tablet server)
	 * 
	 * @return
	 */
	@Override
	public K lastKey() {
		K k = Collections.max(deriveMap(new CountsDerivedMapper()).keySet(), comparator());
		return k;
	}
	/**
	 * @return a local in memory TreeMap copy containing this ENTIRE map 
	 */
	public final TreeMap<K,V> localCopy(){
		TreeMap<K,V> local = new TreeMap<K,V>();
		local.putAll(this);
		return local;
	}


	/**
	 * make a ForiegnKey to a key in this table
	 * @param key
	 * @return
	 */
	public ForeignKey<V> makeForeignKey(Object key) {
		return new ForeignKey<V>(conn,null,getTable(),key);
	}
	public Iterator<Entry<K,V>> multiEntryIterator(){
		Scanner	s;
		try {
			// get scanner without versioning iterator
			s = getMultiScanner();
			Iterator<Entry<Key, Value>> it = s.iterator();
			return new EntrySetIterator(it);
		} catch (TableNotFoundException e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	/**
	 * submaps may pile on iterators to chain of getScanner. this method returns the next iterator priority
	 */
	protected int nextIteratorPriority(){
		return DERIVEDMAP_ITERATOR_PRIORITY_MIN;
	}

	@Override
	public V put(K key, V value) {
		if(isReadOnly())
			throw new UnsupportedOperationException();
		try {
			V prev = this.get(key);
			putWithoutGet(key,value);
			return prev;
		}
		catch(Exception e){
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}


	/**
	 * this method is optimized for batch writing. 
	 * it should be faster than repeated calls to put(), which flushes BatchWriter after each entry
	 */

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		if(isReadOnly())
			throw new UnsupportedOperationException();
		importAll(m.entrySet().iterator(),null,false);
	}
	public void putAll(Map<? extends K, ? extends V> m, KeyValueTransformer trans) {
		if(isReadOnly())
			throw new UnsupportedOperationException();
		importAll(m.entrySet().iterator(),trans,false);
	}

	/**
	 * same as put(), but doesn't fetch the previous value
	 * @param key
	 * @param value
	 */
	public void putWithoutGet(K key, V value) {
		if(isReadOnly())
			throw new UnsupportedOperationException();
		try {
			BatchWriter bw = getBatchWriter();
			addPutMutation(key, value,bw);
			bw.flush();
		}
		catch(MutationsRejectedException e){
			log.error(e.getMessage());
			try {
				reinitBatchWriter();
				throw new RuntimeException(e);
			} catch (Exception e1) {
				log.error(e1.getMessage());
				throw new RuntimeException(e1);
			}
		}
	}
	
	public AccumuloSortedMap<K, V> regexFilter(String keyRegex,
			String valueRegex) {
		Map<String,String> cfg = new HashMap<String,String>();
		if(keyRegex != null)
			cfg.put(RegexFilter.OPT_KEYREGEX, keyRegex);
		if(valueRegex != null)
			cfg.put(RegexFilter.OPT_VALUEREGEX, valueRegex);
		configureSerdes(cfg,getValueSerde());
		return new IteratorStackedSubmap<K,V>(this,RegexFilter.class,cfg,getValueSerde());
	}
	public final AccumuloSortedMap<K, V>  regexKeyFilter(String keyRegex){
		return regexFilter(keyRegex,null);
	}

	public final AccumuloSortedMap<K, V>  regexValueFilter(String valueRegex){
		return regexFilter(null,valueRegex);
	}

	/*
	 * from https://accumulo.apache.org/1.7/apidocs/org/apache/accumulo/core/client/BatchWriter.html
	 * 
	 * In the event that an MutationsRejectedException exception is thrown by one of the methods on a BatchWriter instance, the user should close the current instance and create a new instance. 
	 * This is a known limitation which will be addressed by ACCUMULO-2990 in the future.
	 */
	protected void reinitBatchWriter() throws TableNotFoundException, MutationsRejectedException{
		if(batchWriter != null)
			batchWriter.close();
		batchWriter = getConnector().createBatchWriter(getTable(), getBatchWriterConfig());
	}

	@Override
	public V remove(Object key) {
		if(isReadOnly())
			throw new UnsupportedOperationException();
		try {
			V prev = this.get(key);
			BatchWriter bw = getBatchWriter();
			addRemoveMutation(key,bw);
			bw.flush();
			return prev;
		}
		catch(MutationsRejectedException e){
			log.error(e.getMessage());
			try {
				reinitBatchWriter();
				throw new RuntimeException(e);
			} catch (Exception e1) {
				log.error(e1.getMessage());
				throw new RuntimeException(e1);
			}
		}
	}
	/**
	 * resolves a ForeignKey using the connector of this map
	 * @param fk
	 * @return
	 * @throws AccumuloSecurityException 
	 * @throws AccumuloException 
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public Object resolve(ForeignKey fk) throws InstantiationException, IllegalAccessException, ClassNotFoundException, AccumuloException, AccumuloSecurityException{
		if(fk == null || conn == null)
			return null;
		fk.setConnector(conn);
		return fk.resolve();
	}
	
	public AccumuloSortedMap<K,StatisticalSummary> rowStats(){
		return (AccumuloSortedMap<K,StatisticalSummary>) deriveMap(new RowStatsMapper(),true);
	}
	public final AccumuloSortedMap<K, V> sample(Sampler s){
		return sample(s.samplingSeed,s.fromFractionalHash,s.toFractionalHash,s.fromTs,s.toTs);
	}
	
	public final AccumuloSortedMap<K, V> sample(final double fraction){
		return sample(Util.randomHexString(DEFAULT_RANDSEED_LENGTH),0,fraction,-1, -1);
	}

	public final AccumuloSortedMap<K, V> sample(final String randSeed, final double from_fraction, final double to_fraction){
		return sample(randSeed,from_fraction,to_fraction,-1,-1);		
	}

	public AccumuloSortedMap<K, V> sample(final String randSeed,final double from_fraction, final double to_fraction, final long min_timestamp, final long max_timestamp){
		Map<String,String> cfg = new HashMap<String,String>();
		cfg.put(SamplingFilter.OPT_FROMFRACTION, Double.toString(from_fraction));
		cfg.put(SamplingFilter.OPT_TOFRACTION, Double.toString(to_fraction));
		cfg.put(SamplingFilter.OPT_RANDOMSEED, randSeed);
		if(max_timestamp > 0)
			cfg.put(SamplingFilter.OPT_MAXTIMESTAMP, Long.toString(max_timestamp));
		if(min_timestamp > 0)
			cfg.put(SamplingFilter.OPT_MINTIMESTAMP, Long.toString(min_timestamp));
		return new IteratorStackedSubmap<K,V>(this,SamplingFilter.class,cfg,getValueSerde());
	}
	public void setBatchWriterConfig(BatchWriterConfig batchWriterConfig) {
		this.batchWriterConfig = batchWriterConfig;
		try {
			reinitBatchWriter();
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}
	/**
	 * also sets readonly to false if clearable = true
	 * @param clearable
	 */
	public AccumuloSortedMap<K, V>  setClearable(boolean clearable) {
		this.clearable = clearable;
		if(clearable)
			setReadOnly(false);
		return this;
	}
	
	/**
	 * sets the column family of values
	 * @param cf
	 * @return
	 */
	protected AccumuloSortedMap<K, V> setColumnFamily(byte[] cf){
		colfam = cf;
		return this;
	}
	/**
	 * sets the column qualifier of values
	 * @param cf
	 * @return
	 */
	protected AccumuloSortedMap<K, V> setColumnQualifier(byte[] cq){
		colqual = cq;
		return this;
	}	
	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#setColumnVisibility(byte[])
	 */
	public AccumuloSortedMap<K, V> setColumnVisibility(byte[] cv){
		colvis = cv;
		return this;
	}
	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#setKeySerde(com.isentropy.accumulo.collections.io.SerDe)
	 */
	public AccumuloSortedMap<K, V> setKeySerde(SerDe s){
		keySerde=s;
		return this;
	}

	
	/**
	 * This method enables one-to-many mapping. It uses Accumulo's VersioningIterator. 
	 * There is no way to delete a single value. You can only delete the key. 
	 * See multiEntryIterator() and getAll(key), which read the multiple values.
	 * 
	 * @param max_values_per_key the maximum number of values that will be persisted for each key, or -1 for unlimited
	 * @return
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 * @throws TableNotFoundException
	 */
	public AccumuloSortedMap<K, V> setMaxValuesPerKey(int max_values_per_key) throws AccumuloSecurityException, AccumuloException, TableNotFoundException{
		if(isReadOnly())
			throw new UnsupportedOperationException("must set multiMap on base map, not derived map");

		int existingMM = getMaxValuesPerKey(); 
		if(max_values_per_key == existingMM){
			log.info("setMultiMap doing nothing because max_values_per_key is already set to "+existingMM);
			return this;
		}
		
		EnumSet<IteratorScope> all = EnumSet.allOf(IteratorScope.class);
		//this.max_values_per_key = max_values_per_key;
		if(getConnector().tableOperations().listIterators(getTable()).containsKey(ITERATOR_NAME_VERSIONING)){
			getConnector().tableOperations().removeIterator(getTable(), ITERATOR_NAME_VERSIONING, all);
			log.info("Removed versioning iterator for table "+getTable());
		}

		if(max_values_per_key > 0){
			IteratorSetting is = new IteratorSetting(ITERATOR_PRIORITY_VERSIONING,ITERATOR_NAME_VERSIONING,VersioningIterator.class);
			VersioningIterator.setMaxVersions(is, max_values_per_key);
			log.info("attaching versioning iterator for table "+getTable()+ " with max version = "+max_values_per_key);
			getConnector().tableOperations().attachIterator(getTable(), is, all);
		}
		return this;
	}


	public AccumuloSortedMap<K, V> setReadOnly(boolean ro){
		readOnly = ro;
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
	public AccumuloSortedMap<K, V> setTimeOutMs(long timeout){
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
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#setValueSerde(com.isentropy.accumulo.collections.io.SerDe)
	 */
	public AccumuloSortedMap<K, V> setValueSerde(SerDe s){
		valueSerde=s;
		return this;
	}

	/**
	 * included for compatibility with java Map interface.
	 * sizeAsLong() is preferred, since the map size may exceed Integer.MAX_VALUE
	 * 
	 * returns -1 if true size is greater than Integer.MAX_VALUE
	 */
	@Override
	public int size(){
		long sz = sizeAsLong();
		if(sz > Integer.MAX_VALUE){
			log.warn("True map size exceeds Integer.MAX_VALUE. Map.size() returning -1. Use sizeAsLong() to get true size.");
			return -1;
		}
		return (int) sz;
	}



	/* (non-Javadoc)
	 * @see com.isentropy.accumulo.collections.AccumuloSortedMapIF#sizeAsLong()
	 */
	public long sizeAsLong(boolean countMultipleValues){
		return MapAggregates.count(this,countMultipleValues);
	}
	public long sizeAsLong(){
		return sizeAsLong(false);
	}
	/**
	 * the end include booleans DONT WORK currently because of bug in Accumulo:
	 * 
	 * "There is a Java proxy bug in the scanner. If you create a BatchScanner, every range assigned to it has startInclusive=true, endInclusive=false."
	 * from https://github.com/accumulo/pyaccumulo/issues/14
	 * 
	 * keeping this method protected until the bug is fixed
	 * 
	 * @param fromKey
	 * @param inc1
	 * @param toKey
	 * @param inc2
	 * @return
	 */
	protected AccumuloSortedMap<K, V> subMap(final K fromKey,final boolean inc1,final K toKey,final boolean inc2) {
		final AccumuloSortedMap<K, V> parent = this;
		Scanner s;
		try {
			s = parent.getScanner();
		} catch (TableNotFoundException e) {
			throw new RuntimeException(e);
		}

		Range curRange = s.getRange();

		Key fk = getKey(fromKey);
		if(fk != null)
			fk.setTimestamp(inc1?Long.MAX_VALUE:0);

		Key tk = getKey(toKey);
		if(tk != null)
			tk.setTimestamp(inc2?0:Long.MAX_VALUE);

		Range requestedRange = new Range(fk,inc1,tk,inc2);
		final Range newRange = curRange == null ? requestedRange : curRange.clip(requestedRange,true);
		//ranges are disjoint
		if(newRange == null)
			return new EmptyAccumuloSortedMap<K,V>();
		return new Submap(newRange,this,getValueSerde());

	}
	/**
	 * null accepted as no limit. returned map is READ ONLY
	 * @param fromKey
	 * @param toKey
	 * @return
	 */

	@Override
	public final AccumuloSortedMap<K, V> subMap(K fromKey, K toKey) {
		return subMap(fromKey,true,toKey,false);
	}

	@Override
	public final AccumuloSortedMap<K, V> tailMap(K fromKey) {
		return subMap(fromKey,true,null,true);
	}


	public final AccumuloSortedMap<K, V> timeFilter(long min_timestamp, long max_timestamp){
		return sample("",0,1,min_timestamp,max_timestamp);
	}
	
	/**
	 * computes a StatisticalSummary of all values that are of java Numbers
	 * @param includeMultipleValues
	 * @return
	 */
	public StatisticalSummary valueStats(boolean includeMultipleValues){
		return MapAggregates.valueStats(this, includeMultipleValues);
	}

	public StatisticalSummary valueStats(){
		return valueStats(false);
	}
	
	@Override
	public Collection<V> values() {
		final AccumuloSortedMap<K,V> parent = this;

		return new Collection<V>(){

			@Override
			public boolean add(V e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean addAll(Collection<? extends V> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void clear() {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean contains(Object o) {
				return parent.containsValue(o);
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
			public boolean isEmpty() {
				return size() == 0;
			}

			@Override
			public Iterator<V> iterator() {
				return new ValueSetIterator(parent.iterator());
			}

			@Override
			public boolean remove(Object o) {
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
			public int size() {
				long sz = parent.size();
				if(sz > Integer.MAX_VALUE){
					log.warn("True map size exceeds MAX_INT. Map.size() returning -1. Use sizeAsLong() to get true size.");
					return -1;
				}
				return (int) sz;
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
		};
	}

	public final V waitFor(K key, long maxms) throws InterruptedException{
		return waitFor(key,maxms,DEFAULT_WAIT_MS);
	}
	public final V waitFor(K key, long maxms, long incms) throws InterruptedException{
		long waited =0;
		V val=null;
		while((val=get(key)) == null && waited <= maxms){
			long waitTime = incms <= maxms - waited ? incms : maxms - waited;
			log.debug("waitFor wating "+waitTime+"ms");
			Thread.sleep(waitTime);
			waited += waitTime;
		}
		return val;
	}



}
