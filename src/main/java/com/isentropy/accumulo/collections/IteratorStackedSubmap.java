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

import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;

import com.isentropy.accumulo.collections.io.SerDe;
import com.isentropy.accumulo.iterators.SamplingFilter;

public class IteratorStackedSubmap<K,V> extends ScannerDerivedMap<K,V>{
	private Class<? extends SortedKeyValueIterator<Key, Value>> iterator;
	private Map<String,String> iterator_options;
	private boolean isAggregate = false;
	
	public IteratorStackedSubmap(AccumuloSortedMap<K,?> parent, Class<? extends SortedKeyValueIterator<Key, Value>> iterator, Map<String,String> iterator_options, SerDe derivedMapValueSerde) {
		super(parent,derivedMapValueSerde);
		this.iterator = iterator;
		this.iterator_options = iterator_options;
	}
	
	@Override 
	protected int nextIteratorPriority(){
		return parent.nextIteratorPriority()+1;
	}
	
	@Override
	protected Scanner getScanner() throws TableNotFoundException{
		if(isAggregate)
			return getMultiScanner();
		Scanner s = parent.getScanner();
		IteratorSetting cfg = new IteratorSetting(parent.nextIteratorPriority(), iterator);
		cfg.setName(iterator.getSimpleName()+parent.nextIteratorPriority());
		cfg.addOptions(iterator_options);
		s.addScanIterator(cfg);
		return s;
	}
	@Override
	protected Scanner getMultiScanner() throws TableNotFoundException{
		Scanner s = parent.getMultiScanner();
		IteratorSetting cfg = new IteratorSetting(parent.nextIteratorPriority(), iterator);
		cfg.setName(iterator.getSimpleName()+parent.nextIteratorPriority());
		cfg.addOptions(iterator_options);
		s.addScanIterator(cfg);
		return s;
	}
	/**
	 * if true, getScanner() will return getMultiScanner().
	 * This is useful when you want to create a derived map that computes a aggregate objects from
	 * multiple values of its parent map. See setMultiMap(n)
	 * @return
	 */
	public boolean isAggregate() {
		return isAggregate;
	}

	public void setAggregate(boolean isAggregate) {
		this.isAggregate = isAggregate;
	}	
}
