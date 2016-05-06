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

/**
 * derived map that comes from modifying scanner
 */
public abstract class ScannerDerivedMap<K,V> extends AccumuloSortedMap<K,V>{
	protected AccumuloSortedMap<K,?> parent;
	protected SerDe newValueSerde;
	
	public ScannerDerivedMap(AccumuloSortedMap<K,?> parent, SerDe derivedMapValueSerde) {
		this.parent = parent;
		newValueSerde = derivedMapValueSerde;
	}
	
	@Override
	public boolean isReadOnly() {
		return true;
	}
	
	public abstract Scanner getScanner() throws TableNotFoundException;
	
	@Override
	public SerDe getKeySerde() {
		return parent.getKeySerde();
	}
	@Override
	public SerDe getValueSerde() {
		return newValueSerde;
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

}
