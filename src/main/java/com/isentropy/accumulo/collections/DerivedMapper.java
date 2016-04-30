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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.isentropy.accumulo.collections.io.SerDe;

/**
 *  DerivedMapper wraps an iterator with options and a SerDe, and is passed to AccumuloSortedMap.deriveMap() 
 *  which wraps AccumuloSortedMap.derivedMapFromIterator().
 *  
 *  The iterator should not modify keys, as that will cause the derived map to malfunction.
 *  If your iterator creates an aggregate from many key/values, return the last processed key as the key, and the aggregate as the value.
 * 
 *  derivedMapFromIterator() will modify your supplied iterator options by adding 
 *  AccumuloSortedMap.OPT_KEY_SERDE, OPT_VALUE_INPUT_SERDE, OPT_VALUE_OUTPUT_SERDE, 
 *  which specify the classnames of serdes used by this map. Your iterator should use these supplied SerDes,
 *  or just extend DeserializedFilter or DeserializedTransformingIterator, which handle serialization for you.
 *  
 *  The derived map's key SerDe is the same as the parent map.
 *  
 */

public interface DerivedMapper {
	/**
	 * 
	 * @return the iterator that will be stacked to create the derived map. 
	 * Consider extending an existing deserialized iterator like DeserializedFilter or DeserializedTransformingIterator, 
	 * which manage serialization
	 */
	public  Class<? extends SortedKeyValueIterator<Key, Value>> getIterator();

	/**
	 * 
	 * @return extra iterator options. can be null 
	 */
	public Map<String,String> getIteratorOptions();
	
	/**
	 * @return This is the serde that reads the output of the iterator from getIterator().
	 * If null, the map's existing value Serde will be for the values of the derived map.
	 */
	public SerDe getDerivedMapValueSerde();
}
