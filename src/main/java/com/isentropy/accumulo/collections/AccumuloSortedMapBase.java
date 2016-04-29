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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.isentropy.accumulo.collections.io.SerDe;
import com.isentropy.accumulo.util.Util;

public abstract class AccumuloSortedMapBase<K, V> implements SortedMap<K,V>{
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

	/**
	 *
	 *  This method allows 
	 */
	protected abstract AccumuloSortedMapBase<K,V> derivedMapFromIterator(Class<? extends SortedKeyValueIterator<Key, Value>> iterator, Map<String,String> iterator_options, SerDe derivedMapValueSerde);
	
/**
 * wraps derivedMapFromIterator and add iterator options OPT_KEY_SERDE, OPT_VALUE_INPUT_SERDE, OPT_VALUE_OUTPUT_SERDE
 * that specify the classname of the key and value serdes used by this map

 * if mapper.getDerivedMapValueSerde() == null, OPT_VALUE_OUTPUT_SERDE will be set to same value as OPT_VALUE_INPUT_SERDE
   and derivedMapFromIterator will be passed the current map's value serde
 * @param mapper
 * @return
 */
	public AccumuloSortedMapBase<K,V> deriveMap(DerivedMapper mapper){
		Map<String,String> opts = mapper.getIteratorOptions();
		if(opts == null)
			opts = new HashMap<String,String>();
		opts.put(OPT_KEY_SERDE, getKeySerde().getClass().getName());
		opts.put(OPT_VALUE_INPUT_SERDE, getValueSerde().getClass().getName());
		SerDe output_value_serde = mapper.getDerivedMapValueSerde();
		if(output_value_serde != null)
			opts.put(OPT_VALUE_OUTPUT_SERDE, output_value_serde.getClass().getName());
		else
			opts.put(OPT_VALUE_OUTPUT_SERDE, getValueSerde().getClass().getName());
			
		return derivedMapFromIterator(mapper.getIterator(),opts, output_value_serde == null ? getValueSerde():output_value_serde);
	}

	public abstract AccumuloSortedMapBase<K, V> sample(final double from_fraction, final double to_fraction,final String randSeed,long max_timestamp);
	public final AccumuloSortedMapBase<K, V> sample(final double from_fraction, final double to_fraction,final String randSeed){
		return sample(from_fraction,to_fraction,randSeed,-1);		
	}
	public final AccumuloSortedMapBase<K, V> sample(final double fraction){
		return sample(0,fraction,Util.randomHexString(DEFAULT_RANDSEED_LENGTH),-1);
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


}