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

import java.io.Serializable;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;

import com.isentropy.accumulo.collections.factory.AccumuloSortedMapFactory;

/**
 * This class represents a map value that points to a key (ie row) in another map.
 *
 * @param <V>
 */
public class ForeignKey<V> implements Serializable {

	private String factoryName=null,tableName=null;
	private Object key=null;
	private transient Connector conn;
	private transient AccumuloSortedMapFactory fact;
	private transient AccumuloSortedMap map;

	public ForeignKey(Connector c, String factory_name, String table_name,Object key) {
		conn=c;
		factoryName=factory_name;
		tableName=table_name;
		this.key = key;
	}
	public V resolve() throws AccumuloException, AccumuloSecurityException, InstantiationException, IllegalAccessException, ClassNotFoundException{
		if(conn == null)
			return null;
		if(tableName == null)
			return null;
		if(key == null)
			return null;
		if(map == null){
			if(factoryName != null){
				if(fact ==null)
					fact = new AccumuloSortedMapFactory(conn,factoryName);
				map=fact.makeMap(tableName,false);
			}
			else{
				map = new AccumuloSortedMap(conn,tableName,false,false);
			}
		}
		return (V) map.get(key);
	}
	public void setConnector(Connector c){
		conn = c;
	}
	public Connector getConnector(){
		return conn;
	}
	/**
	 * calls link.resolve() if link is a ForeignKey, otherwise null
	 * @param link
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	public static final Object resolve(Object link) throws InstantiationException, IllegalAccessException, ClassNotFoundException, AccumuloException, AccumuloSecurityException{
		if(link == null || !(link instanceof ForeignKey))
			return null;
		return ((ForeignKey) link).resolve();
	}

}
