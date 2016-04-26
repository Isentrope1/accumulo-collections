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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.math3.stat.descriptive.AggregateSummaryStatistics;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.commons.math3.stat.descriptive.StatisticalSummaryValues;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isentropy.accumulo.collections.io.JavaSerializationSerde;
import com.isentropy.accumulo.collections.io.SerDe;
import com.isentropy.accumulo.iterators.AggregateIterator;
import com.isentropy.accumulo.iterators.LongCountAggregateIterator;
import com.isentropy.accumulo.iterators.StatsAggregateIterator;

/**
 * This class contains useful server-side aggregation functions
 *
 */
public class MapAggregates {
	public static Logger log = LoggerFactory.getLogger(MapAggregates.class);

	/**
	 * Returns a statistical summary of all the map values that are instances of Number. It collects SummaryStatistics
	 * for each tablet and aggregates the end results.
	 * @param map
	 * @return
	 */
	public static StatisticalSummary valueStats(AccumuloSortedMapInterface map){
		try {
			return valueStats(map.getScanner(),map.getKeySerde().getClass(),map.getValueSerde().getClass());
		} catch (TableNotFoundException e1) {
			log.error(e1.getMessage());
			return null;
		}
	}
	
	protected static StatisticalSummary valueStats(Scanner s, Class keySerDe, Class valueSerDe){
		try{	
			IteratorSetting cfg = new IteratorSetting(Integer.MAX_VALUE, StatsAggregateIterator.class);
			cfg.addOption(AggregateIterator.OPT_KEYSERDE, keySerDe.getName());
			cfg.addOption(AggregateIterator.OPT_VALUESERDE, valueSerDe.getName());

			s.addScanIterator(cfg);

			List<SummaryStatistics> perTabletStats = new ArrayList<SummaryStatistics>();
			for(Map.Entry<Key, Value> e : s){
				SummaryStatistics stats = (SummaryStatistics) JavaSerializationSerde.javaDeserialize(e.getValue().get());
				perTabletStats.add(stats);
			}
			StatisticalSummaryValues fullstats = AggregateSummaryStatistics.aggregate(perTabletStats);
			return fullstats;
		}
		catch(Exception e){
			log.warn(e.getMessage());
			log.warn("Stats aggregation using tablet server iterator failed. Install accumulo-collections jar on the tablet servers.");
		}
		return null;
	}
	/**
	 * same as map.sizeAsLong()
	 * @param map
	 * @return
	 */
	public static long count(AccumuloSortedMapInterface map){
		try {
			return count(map.getScanner());
		} catch (TableNotFoundException e) {
			log.error(e.getMessage());
			return -1;
		}
	}
	/**
	 * counts total keys
	 * @param s
	 * @return
	 */
	protected static long count(Scanner s){
		try{	
			return countUsingIterator(s);
		}
		catch(Exception e){
			log.warn(e.getMessage());
			log.warn("Table count using tablet server iterator failed. Install accumulo-collections jar on the tablet servers. "+
					" Trying to run count(Scanner s) locally.");
		}
		return countClientSide(s);
	}

	protected static long countClientSide(Scanner s){
		log.warn("countClientSide() is slow. Install accumulo-collections jar on tablet server and use countUsingIterator() instead");
		long cnt = 0;
		for(Map.Entry<Key, Value> e : s){
			cnt++;
		}
		return cnt;
	}
	protected static long countUsingIterator(Scanner s){
		IteratorSetting cfg = new IteratorSetting(Integer.MAX_VALUE, LongCountAggregateIterator.class);
		s.addScanIterator(cfg);
		long cnt = 0;
		for(Map.Entry<Key, Value> e : s){
			log.debug("kv: "+e.getKey()+"\t"+e.getValue());
			cnt += Long.parseLong(e.getValue().toString());
		}
		return cnt;
	}

}
