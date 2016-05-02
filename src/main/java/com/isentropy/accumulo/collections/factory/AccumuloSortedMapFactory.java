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

import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isentropy.accumulo.collections.AccumuloSortedMap;
import com.isentropy.accumulo.collections.AccumuloSortedMapBase;
import com.isentropy.accumulo.collections.io.JavaSerializationSerde;
import com.isentropy.accumulo.collections.io.SerDe;
import com.isentropy.accumulo.collections.io.Utf8Serde;

/**
 * 
 * The AccumuloSortedMapFactory manages map metadata, and maps come out of the factory
 * via makeMap() preconfigured (ie serdes set, etc).
 * 
 * The AccumuloSortedMapFactory is backed by a AccumuloSortedMap of tableName->metadata.
 * 
 * 
 * @param <K>
 * @param <V>
 */
public class AccumuloSortedMapFactory<K,V> {
	public static Logger log = LoggerFactory.getLogger(AccumuloSortedMapFactory.class);

	// default Properties are written as table DEFAULT_SETTING_METATABLENAME
	public static final String DEFAULT_SETTING_METATABLENAME="!default!";

	public static final String MAP_PROPERTY_KEY_SERDE=AccumuloSortedMapBase.OPT_KEY_SERDE;
	public static final String MAP_PROPERTY_VALUE_SERDE=AccumuloSortedMapBase.OPT_VALUE_INPUT_SERDE;

	
	private AccumuloSortedMap<String,Properties> tableNameToProperties;
	String metadataTable;
	Connector conn;
	public AccumuloSortedMapFactory(Connector c, String metadataTable) throws AccumuloException, AccumuloSecurityException {
		conn = c;
		this.metadataTable = metadataTable;
		tableNameToProperties = new AccumuloSortedMap<String,Properties>(c,metadataTable);		
		tableNameToProperties.setKeySerde(new Utf8Serde());
		tableNameToProperties.setValueSerde(new JavaSerializationSerde());
		addDefaultProperty(MAP_PROPERTY_KEY_SERDE, JavaSerializationSerde.class.getName());
		addDefaultProperty(MAP_PROPERTY_VALUE_SERDE, JavaSerializationSerde.class.getName());
	}
	
	protected Properties getProperties(String tableName){
		return tableNameToProperties.get(tableName);
	}
	/**
	 * this sets up the map according to the metadata, setting serdes
	 * @param tableName
	 * @return
	 * @throws AccumuloSecurityException 
	 * @throws AccumuloException 
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public FactoryAccumuloSortedMap<K,V> makeMap(String tableName) throws AccumuloException, AccumuloSecurityException, InstantiationException, IllegalAccessException, ClassNotFoundException{
		FactoryAccumuloSortedMap<K,V> out = new FactoryAccumuloSortedMap<K,V>(conn,tableName);
		Properties props = tableNameToProperties.get(DEFAULT_SETTING_METATABLENAME);
		if(props == null)
			props = new Properties();
		Properties p = getProperties(tableName);
		if(p != null)
			props.putAll(p);
		configureMap(out,props);
		return out;
	}
	
	protected void configureMap(FactoryAccumuloSortedMap<K,V> map, Properties props) throws InstantiationException, IllegalAccessException, ClassNotFoundException{
		String v;
		if((v = props.getProperty(MAP_PROPERTY_KEY_SERDE)) != null){
			map.setKeySerde((SerDe) Class.forName(v).newInstance());
		}
		if((v = props.getProperty(MAP_PROPERTY_VALUE_SERDE)) != null){
			map.setValueSerde((SerDe) Class.forName(v).newInstance());
		}
	}
	
	public void addDefaultProperty(String key,String value){
		addMapSpecificProperty(DEFAULT_SETTING_METATABLENAME,key,value);
	}
	public void addMapSpecificProperty(String tableName,String key,String value){
		Properties p = getProperties(tableName);
		if(p == null){
			p = new Properties();
		}
		p.setProperty(key, value);		
		tableNameToProperties.put(tableName, p);
	}
	public String getMapSpecificProperty(String tableName,String key){
		Properties p = getProperties(tableName);
		if(p == null){
			return null;
		}
		return p.getProperty(key);
	}
	public String getDefaultProperty(String key){
		return getMapSpecificProperty(DEFAULT_SETTING_METATABLENAME,key);
	}

}
