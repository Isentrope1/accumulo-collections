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
import org.apache.accumulo.core.client.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isentropy.accumulo.collections.AccumuloSortedMap;
import com.isentropy.accumulo.collections.io.FixedPointSerde;
import com.isentropy.accumulo.collections.io.JavaSerializationSerde;
import com.isentropy.accumulo.collections.io.SerDe;
import com.isentropy.accumulo.util.Util;

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
public class AccumuloSortedMapFactory {
	public static Logger log = LoggerFactory.getLogger(AccumuloSortedMapFactory.class);

	// default Properties are written as factory table DEFAULT_SETTING_METATABLENAME
	public static final String DEFAULT_SETTING_METATABLENAME="!default!";

	public static final String MAP_PROPERTY_KEY_SERDE=AccumuloSortedMap.OPT_KEY_SERDE;
	public static final String MAP_PROPERTY_VALUE_SERDE=AccumuloSortedMap.OPT_VALUE_INPUT_SERDE;
	public static final String MAP_PROPERTY_TABLE_NAME="table_name";
	public static final String MAP_PROPERTY_VALUES_PER_KEY="values_per_key";
	public static final String MAP_PROPERTY_TTL="ttl";
	
	public static final int RANDOM_TABLE_NAME_LENGTH = 10;

	
	private AccumuloSortedMap<String,Properties> tableAliasToProperties;
	private String metadataTable;
	private Connector conn;
	public AccumuloSortedMapFactory(Connector c, String metadataTable) throws AccumuloException, AccumuloSecurityException {
		conn = c;
		this.metadataTable = metadataTable;
		tableAliasToProperties = new AccumuloSortedMap<String,Properties>(c,metadataTable);		
		tableAliasToProperties.setKeySerde(new FixedPointSerde());
		tableAliasToProperties.setValueSerde(new JavaSerializationSerde());
		addDefaultProperty(MAP_PROPERTY_KEY_SERDE, FixedPointSerde.class.getName());
		addDefaultProperty(MAP_PROPERTY_VALUE_SERDE, FixedPointSerde.class.getName());
	}
	
	protected Properties getProperties(String mapName){
		return tableAliasToProperties.get(mapName);
	}
	protected String createTableName(String mapName){
		return metadataTable+"_"+mapName+"_"+ Util.randomHexString(RANDOM_TABLE_NAME_LENGTH);
	}
	
	public boolean containsMap(String mapName){
		return tableAliasToProperties.containsKey(mapName);
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
	public FactoryAccumuloSortedMap makeMap(String mapName) throws AccumuloException, AccumuloSecurityException, InstantiationException, IllegalAccessException, ClassNotFoundException{
		Properties props = tableAliasToProperties.get(DEFAULT_SETTING_METATABLENAME);
		if(props == null){
			props = new Properties();
		}
		String tableName;
		Properties p = getProperties(mapName);
		if(p != null)
			props.putAll(p);
		if((tableName = props.getProperty(MAP_PROPERTY_TABLE_NAME)) == null){
			tableName = createTableName(mapName);
			props.put(MAP_PROPERTY_TABLE_NAME, tableName);
			tableAliasToProperties.put(mapName, props);
		}
		
		FactoryAccumuloSortedMap out = new FactoryAccumuloSortedMap(conn,tableName,true);
		try {
			configureMap(out,props,mapName);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new AccumuloException(e);
		}
		out.isInitialized = true;
		return out;
	}
	public FactoryAccumuloSortedMap makeReadOnlyMap(String mapName) throws AccumuloException, AccumuloSecurityException, InstantiationException, IllegalAccessException, ClassNotFoundException{
		FactoryAccumuloSortedMap map = makeMap(mapName);
		map.setReadOnly(true);
		return map;
	}
	
	protected void configureMap(FactoryAccumuloSortedMap map, Properties props,String tableAlias) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NumberFormatException, AccumuloSecurityException, AccumuloException, TableNotFoundException{
		String v;
		if((v = props.getProperty(MAP_PROPERTY_KEY_SERDE)) != null){
			map.setKeySerde((SerDe) Class.forName(v).newInstance());
		}
		if((v = props.getProperty(MAP_PROPERTY_VALUE_SERDE)) != null){
			map.setValueSerde((SerDe) Class.forName(v).newInstance());
		}
		if((v = props.getProperty(MAP_PROPERTY_VALUES_PER_KEY)) != null){
				map.setMaxValuesPerKey(Integer.parseInt(v));
		}
		if((v = props.getProperty(MAP_PROPERTY_TTL)) != null){
			map.setTimeOutMs(Long.parseLong(v));
		}
		map.setTableAlias(tableAlias);
		map.setFactoryName(metadataTable);
	}
	
	public void addDefaultProperty(String key,String value){
		addMapSpecificProperty(DEFAULT_SETTING_METATABLENAME,key,value);
	}
	public void removeMapSpecificProperty(String tableName,String key){
		Properties p = getProperties(tableName);
		if(p != null){
			p.remove(key);
		}
	}
	public void removeDefaultProperty(String key){
		removeMapSpecificProperty(DEFAULT_SETTING_METATABLENAME,key);
	}
	public void addMapSpecificProperty(String tableName,String key,String value){
		Properties p = getProperties(tableName);
		if(p == null){
			p = new Properties();
		}
		p.setProperty(key, value);		
		tableAliasToProperties.put(tableName, p);
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
