package com.isentropy.accumulo.collections;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.isentropy.accumulo.collections.io.SerDe;

/**
 *  DerivedMapper wraps an iterator with options and a SerDe, and is passed to AccumuloSortedMap.deriveMap() 
 *  which wraps AccumuloSortedMap.derivedMapFromIterator().
 * 

	 derivedMapFromIterator() will modify your supplied iterator options by adding
	 AccumuloSortedMap.OPT_KEY_SERDE, OPT_VALUE_INPUT_SERDE, OPT_VALUE_OUTPUT_SERDE, 
	 which specify the classnames of serdes used by this map. 
	 
	 Your iterator should use these supplies SerDes.

 */

public interface DerivedMapper {
	public  Class<? extends SortedKeyValueIterator<Key, Value>> getIterator();
	public Map<String,String> getIteratorOptions();
	/**
	 * can be null. if null, the map's existing value Serde will be used in the derived map
	 */
	public SerDe getDerivedMapValueSerde();
}
