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



package com.isentropy.accumulo.iterators;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.isentropy.accumulo.collections.AccumuloSortedMap.OPT_KEY_SERDE;
import static com.isentropy.accumulo.collections.AccumuloSortedMap.OPT_VALUE_INPUT_SERDE;

import com.isentropy.accumulo.collections.io.SerDe;
import com.isentropy.accumulo.util.KeyValue;
/**
 * 
 * AggregateIterator is an iterator that calculates a tablet-wide aggregate of some sort (count, sum ,etc). 
 *
 */
public abstract class AggregateIterator extends WrappingIterator implements OptionDescriber {
	public static Logger log = LoggerFactory.getLogger(AggregateIterator.class);

	private Key result_key = null;
	private Value result = null;
	private boolean isAggregated = false;
	private boolean isNexted = false;


	// serde can be null if the iterator doesn't actually need to interpret values (eg for count)
	protected SerDe key_serde = null;
	protected SerDe value_serde = null;


	/**
	 * 
	 * @return a KeyValue(last key seen, aggregate object)
	 * @throws IOException
	 */
	protected abstract KeyValue aggregate() throws IOException;

	protected void aggregateWrapper() throws IOException{
		KeyValue kv = aggregate();
		result_key = kv.getKey();
		result = kv.getValue();
		isAggregated = true;
	}

	@Override 
	public boolean hasTop(){
		return !isNexted && result_key != null;
	}

	@Override 
	public void next(){
		isNexted = true;
	}
	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		super.seek(range, columnFamilies, inclusive);
		aggregateWrapper();
	}

	@Override
	public Key getTopKey() {
		//result_key contains the computed aggregate
		return result_key;
	}
	@Override
	public IteratorOptions describeOptions() {
		String desc = "An iterator that sees all rows on a tablet server and computes some aggregate (count, sum, etc)";
		HashMap<String,String> namedOptions = new HashMap<String,String>();
		namedOptions.put(OPT_KEY_SERDE, "serde for keys. only needed if parsing key bytes");
		namedOptions.put(OPT_VALUE_INPUT_SERDE, "serde for values. only needed if parsing value bytes");
		return new IteratorOptions(getClass().getSimpleName(), desc, namedOptions, null);
	}

	@Override
	public boolean validateOptions(Map<String,String> options) {
		String o;
		if((o = options.get(OPT_KEY_SERDE)) != null){
			try{
				SerDe s= (SerDe) Class.forName(o).newInstance();
			}
			catch(Exception e){
				log.error("Error instantiating SerDe "+o+": "+e.getMessage());
				return false;
			}
		}
		if((o = options.get(OPT_VALUE_INPUT_SERDE)) != null){
			try{
				SerDe s= (SerDe) Class.forName(o).newInstance();
			}
			catch(Exception e){
				log.error("Error instantiating SerDe "+o+": "+e.getMessage());
				return false;
			}
		}
		return true;
	}
	@Override
	public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
		super.init(source,options,env);
		String o;
		try{
			if((o = options.get(OPT_KEY_SERDE)) != null)
				key_serde = (SerDe) Class.forName(o).newInstance();
			if((o = options.get(OPT_VALUE_INPUT_SERDE)) != null)
				value_serde = (SerDe) Class.forName(o).newInstance();
		}
		catch(Exception e){
			throw new IOException(e);
		}
	}




	@Override
	public Value getTopValue() {
		return result;
	}
}
