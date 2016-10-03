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
package com.isentropy.accumulo.collections.factory;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;

import com.isentropy.accumulo.collections.AccumuloSortedMap;
import com.isentropy.accumulo.collections.ForeignKey;
import com.isentropy.accumulo.collections.io.SerDe;

/**
 * This class is like AccumuloSortedMap, but throws exceptions if you try to set serdes multiple times
 *
 * @param <K>
 * @param <V>
 */
public class FactoryAccumuloSortedMap<K,V> extends AccumuloSortedMap<K, V> {
	protected String factoryName,tableAlias;
	protected boolean isInitialized=false;
	
	public FactoryAccumuloSortedMap(Connector c, String table) throws AccumuloException, AccumuloSecurityException{
		super(c,table);
	}
	public FactoryAccumuloSortedMap(Connector c, String table, boolean create) throws AccumuloException, AccumuloSecurityException{
		super(c,table,create,false);
	}
	
	@Override
	public AccumuloSortedMap<K, V> setKeySerde(SerDe s){
		if(isInitialized)
			throw new UnsupportedOperationException("cant call setKeySerde on initialized factory map. set factory property "+AccumuloSortedMapFactory.MAP_PROPERTY_KEY_SERDE);		
		return super.setKeySerde(s);
	}
	@Override
	public AccumuloSortedMap<K, V> setValueSerde(SerDe s){
		if(isInitialized)
			throw new UnsupportedOperationException("cant call setValueSerde on initialized factory map. set factory property "+AccumuloSortedMapFactory.MAP_PROPERTY_VALUE_SERDE);		
		return super.setValueSerde(s);
	}
	@Override
	public AccumuloSortedMap<K, V> setMultiMap(int mm) throws AccumuloSecurityException, AccumuloException, TableNotFoundException{
		if(isInitialized)
			throw new UnsupportedOperationException("cant call setMultiMap on initialized factory map. set factory property "+AccumuloSortedMapFactory.MAP_PROPERTY_VALUES_PER_KEY + 
					" to setMultiMap in factory");
		return super.setMultiMap(mm);
	}
	
	public String getFactoryName(){
		return factoryName;
	}
	protected void setFactoryName(String name){
		factoryName = name;
	}
	
	public String getTableAlias(){
		return tableAlias;
	}
	protected void setTableAlias(String name){
		tableAlias = name;
	}
	
	
	@Override
	public ForeignKey makeForeignKey(Object key) {
		return new ForeignKey(getConnector(),factoryName,getTableAlias(),key);
	}

}
