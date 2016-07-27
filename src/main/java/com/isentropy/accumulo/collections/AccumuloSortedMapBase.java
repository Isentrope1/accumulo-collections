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
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import jline.internal.Log;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.isentropy.accumulo.collections.Link.resolve;

import com.isentropy.accumulo.collections.io.SerDe;
import com.isentropy.accumulo.collections.transform.InvertKV;
import com.isentropy.accumulo.collections.transform.KeyValueTransformer;
import com.isentropy.accumulo.util.Util;

public abstract class AccumuloSortedMapBase<K, V> implements SortedMap<K,V>{

	public static Logger log = LoggerFactory.getLogger(AccumuloSortedMapBase.class);

	public static long DEFAULT_WAIT_MS = 1000;


	/*
	 * iterators used in deriveMap will be passed SerDe classname info via these iterator params
	 */
	public static final String OPT_KEY_SERDE = "key_serde";
	public static final String OPT_VALUE_INPUT_SERDE = "value_input_serde";
	public static final String OPT_VALUE_OUTPUT_SERDE = "value_output_serde";


	private static final int DEFAULT_RANDSEED_LENGTH=20;

	public abstract boolean isReadOnly();
	public  abstract SerDe getKeySerde();
	public  abstract AccumuloSortedMapBase<K, V> setKeySerde(SerDe s);
	public  abstract SerDe getValueSerde();
	public  abstract AccumuloSortedMapBase<K, V> setValueSerde(SerDe s);
	public  abstract String getTable();

	/**
	 * sets the column visibility of values
	 * @param cf
	 * @return
	 */
	public  abstract AccumuloSortedMapBase<K, V> setColumnVisibility(byte[] cv);

	public abstract long sizeAsLong();


	/**
	 * deletes the map from accumulo!
	 * @throws TableNotFoundException 
	 * @throws AccumuloSecurityException 
	 * @throws AccumuloException 
	 */
	public abstract void delete() throws AccumuloException, AccumuloSecurityException,
	TableNotFoundException;

	/**
	 * Equivalent to: delete(); createTable();
	 */
	public abstract void clear();

	/**
	 * 
	 * @param key
	 * @return the accumulo timestamp (mod time) of the key of -1 if key is absent
	 */
	public abstract long getTimestamp(K key);

	protected abstract AccumuloSortedMapBase<K,?> derivedMapFromIterator(Class<? extends SortedKeyValueIterator<Key, Value>> iterator, Map<String,String> iterator_options, SerDe derivedMapValueSerde);

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
	public AccumuloSortedMapBase<K,?> deriveMap(DerivedMapper mapper){
		SerDe output_value_serde = mapper.getDerivedMapValueSerde();
		Map<String,String> opts = configureSerdes(mapper.getIteratorOptions(),output_value_serde);
		return derivedMapFromIterator(mapper.getIterator(),opts, output_value_serde == null ? getValueSerde():output_value_serde);
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

	public abstract AccumuloSortedMapBase<K, V> sample(final double from_fraction, final double to_fraction,final String randSeed,long min_timestamp, long max_timestamp);
	public final AccumuloSortedMapBase<K, V> sample(final double from_fraction, final double to_fraction,final String randSeed){
		return sample(from_fraction,to_fraction,randSeed,-1,-1);		
	}
	public final AccumuloSortedMapBase<K, V> sample(final double fraction){
		return sample(0,fraction,Util.randomHexString(DEFAULT_RANDSEED_LENGTH),-1, -1);
	}


	public final AccumuloSortedMapBase<K, V> timeFilter(long min_timestamp, long max_timestamp){
		return sample(0,1,"",min_timestamp,max_timestamp);
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
	protected abstract AccumuloSortedMapBase<K, V> subMap(final K fromKey,final boolean inc1,final K toKey,final boolean inc2);


	@Override
	public final AccumuloSortedMapBase<K, V> subMap(K fromKey, K toKey) {
		return subMap(fromKey,true,toKey,false);
	}

	@Override
	public final AccumuloSortedMapBase<K, V> headMap(K toKey) {
		return subMap(null,true,toKey,false);
	}

	@Override
	public final AccumuloSortedMapBase<K, V> tailMap(K fromKey) {
		return subMap(fromKey,true,null,true);
	}

	/**
	 * a full map checksum to ensure data integrity
	 * computed on tablet servers using MapChecksumAggregateIterator
	 * 
	 * @return a long whose top int is the sum of keys' hashCode() and 
	 *  whose bottom int is the sum of values' hashCode(). Note that this
	 *  checksum runs on the deserialized java objects and should therefore be
	 *  independent of SerDe
	 */
	public long checksum(){
		return MapAggregates.checksum(this);
	}
	public final int checksumKeys(){
		return (int) (checksum() >>> 32);
	}
	public final int checksumValues(){
		return (int) checksum();
	}

	/**
	 * dumps key/values to stream. for debugging
	 * @param ps
	 */

	public void dump(PrintStream ps){
		for(Map.Entry<K,V> e : entrySet()){
			ps.println("k = "+e.getKey()+" : v = "+e.getValue());
		}
		ps.flush();
	}
	/**
	 * @return a local in memory TreeMap copy containing this ENTIRE map 
	 */
	public final TreeMap<K,V> localCopy(){
		TreeMap<K,V> local = new TreeMap<K,V>();
		local.putAll(this);
		return local;
	}
	
	public abstract Link<V> makeLink(Object key);
	
	public Object getResolvedLink(K key) throws InstantiationException, IllegalAccessException, ClassNotFoundException, AccumuloException, AccumuloSecurityException{
		return resolve(get(key));
	}
	
	public abstract AccumuloSortedMapBase<K, V>  regexFilter(String keyRegex,String valueRegex);

	public final AccumuloSortedMapBase<K, V>  regexValueFilter(String valueRegex){
		return regexFilter(null,valueRegex);
	}
	public final AccumuloSortedMapBase<K, V>  regexKeyFilter(String keyRegex){
		return regexFilter(keyRegex,null);
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