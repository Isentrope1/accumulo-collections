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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.isentropy.accumulo.collections.io.LongBinarySerde;
import com.isentropy.accumulo.collections.io.SerDe;
import com.isentropy.accumulo.collections.mappers.ChecksumMapper;
import com.isentropy.accumulo.collections.mappers.CountsDerivedMapper;
import com.isentropy.accumulo.collections.mappers.StatsDerivedMapper;
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
	public static StatisticalSummary valueStats(AccumuloSortedMapBase map){
		AccumuloSortedMapBase tabletSummaries = map.deriveMap(new StatsDerivedMapper());
		List<SummaryStatistics> perTabletStats = new ArrayList<SummaryStatistics>();
		Set<Map.Entry> s =tabletSummaries.entrySet();
		for(Map.Entry e : s){
			SummaryStatistics stats = (SummaryStatistics) e.getValue();
			perTabletStats.add(stats);
		}
		StatisticalSummaryValues fullstats = AggregateSummaryStatistics.aggregate(perTabletStats);
		return fullstats;
	}

	public static long count(AccumuloSortedMapBase map){
		AccumuloSortedMapBase tabletCounts = map.deriveMap(new CountsDerivedMapper());
		Set<Map.Entry> s = tabletCounts.entrySet();
		long sum=0;
		for(Map.Entry e : s){
			sum += (Long) e.getValue();
		}
		return sum;
	}
	public static long checksum(AccumuloSortedMapBase map){
		AccumuloSortedMapBase tabletChecksums = map.deriveMap(new ChecksumMapper());
		Set<Map.Entry> s = tabletChecksums.entrySet();
		long checksum=0;
		for(Map.Entry e : s){
			checksum ^= (Long) e.getValue();
		}
		return checksum;
	}

}
