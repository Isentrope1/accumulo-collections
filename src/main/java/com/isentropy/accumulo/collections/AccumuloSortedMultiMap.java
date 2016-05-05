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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.VersioningIterator;

import com.isentropy.accumulo.collections.io.SerDe;
import com.isentropy.accumulo.collections.mappers.RowStatsMapper;

public class AccumuloSortedMultiMap<K,V> extends AccumuloSortedMap<K,V> {
	protected static final int ITERATOR_PRIORITY_VERSIONING = 20;
	//"vers" is the default VersioningIterator name in Accumulo
	protected static final String ITERATOR_NAME_VERSIONING = "vers";
	protected int max_values_per_key=-1;

	public AccumuloSortedMultiMap(Connector c, String table,int max_versions)
			throws AccumuloException, AccumuloSecurityException {
		super(c, table);
		try {
			this.setMultiMap(max_versions);
		} catch (TableNotFoundException e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}
	public AccumuloSortedMultiMap(Connector c, String table)
			throws AccumuloException, AccumuloSecurityException {
		this(c, table,-1);
	}
	@Override
	protected int nextIteratorPriority(){
		return super.nextIteratorPriority()+1;
	}
	/**
	 * this scanner sees 1 value per key. getMultiScanner() sees multiple
	 */
	@Override
	protected Scanner getScanner() throws TableNotFoundException{
		EnumSet<IteratorScope> all = EnumSet.allOf(IteratorScope.class);
		Scanner s = super.getScanner();
		int prior = super.nextIteratorPriority();
		IteratorSetting is = new IteratorSetting(prior,ITERATOR_NAME_VERSIONING+prior,VersioningIterator.class);
		VersioningIterator.setMaxVersions(is, 1);
		s.addScanIterator(is);
		return s;
	}

	protected Scanner getMultiScanner() throws TableNotFoundException{
		return super.getScanner();
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
			s = super.getScanner();
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

	public Iterator<Entry<K,V>> multiEntryIterator(){
		Scanner	s;
		try {
			// get scanner without versioning iterator
			s = super.getScanner();
			Iterator<Entry<Key, Value>> it = s.iterator();
			return new EntrySetIterator(it);
		} catch (TableNotFoundException e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Override
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
				ps.println("k = "+k+" : v = "+e.getValue());				
			}
		}
		ps.flush();
	}
	
	
	protected AccumuloSortedMap<K,V> derivedMultiMapFromIterator(Class<? extends SortedKeyValueIterator<Key, Value>> iterator, Map<String,String> iterator_options, SerDe derivedMapValueSerde){
		Map<String,String> itcfg = new HashMap<String,String>();
		if(iterator_options != null)
			itcfg.putAll(iterator_options);
		return new IteratorStackedSubmap<K,V>(this,iterator,itcfg,derivedMapValueSerde){
			@Override
			protected Scanner getScannerFromParent() throws TableNotFoundException{
				return getMultiScanner();
			}
		};		
	}
	
	public AccumuloSortedMap<K,V> rowStats(){
		return deriveMultiMap(new RowStatsMapper());
	}

	public AccumuloSortedMap<K,V> deriveMultiMap(DerivedMapper mapper){
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

		return derivedMultiMapFromIterator(mapper.getIterator(),opts, output_value_serde == null ? getValueSerde():output_value_serde);
	}

	public AccumuloSortedMapBase<K, V> setMultiMap(int max_values_per_key) throws AccumuloSecurityException, AccumuloException, TableNotFoundException{
		EnumSet<IteratorScope> all = EnumSet.allOf(IteratorScope.class);
		this.max_values_per_key = max_values_per_key;


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
}
