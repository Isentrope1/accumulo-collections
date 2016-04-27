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

import java.io.PrintStream;
import java.util.SortedMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;

import com.isentropy.accumulo.collections.io.SerDe;

public interface AccumuloSortedMapInterface<K, V> extends SortedMap<K,V>{

	public boolean isReadOnly();
	public SerDe getKeySerde();

	public AccumuloSortedMapInterface<K, V> setKeySerde(SerDe s);

	public SerDe getValueSerde();

	public AccumuloSortedMapInterface<K, V> setValueSerde(SerDe s);

	public String getTable();

	/**
	 * sets the column visibility of values
	 * @param cf
	 * @return
	 */
	public AccumuloSortedMapInterface<K, V> setColumnVisibility(byte[] cv);

	/**
	 * 
	 * @param timeout the entry timeout in ms. If timeout <= 0, the ageoff feature will be removed
	 * @return
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 * @throws TableNotFoundException
	 */

	public AccumuloSortedMapInterface<K, V> setTimeOutMs(long timeout);

	public long sizeAsLong();

	/**
	 * dumps key/values to stream. for debugging
	 * @param ps
	 */
	public void dump(PrintStream ps);

	/**
	 * deletes the map from accumulo!
	 * @throws TableNotFoundException 
	 * @throws AccumuloSecurityException 
	 * @throws AccumuloException 
	 */
	public void delete() throws AccumuloException, AccumuloSecurityException,
			TableNotFoundException;

	/**
	 * Equivalent to: delete(); createTable();
	 */
	public void clear();

	/**
	 * 
	 * @return a Scanner over all rows visible to this map
	 * @throws TableNotFoundException
	 */
	public Scanner getScanner() throws TableNotFoundException;

	public AccumuloSortedMapInterface<K, V> sample(double fraction);

	public AccumuloSortedMapInterface<K, V> sample(double from_fraction,
			double to_fraction, String randSeed);

	public AccumuloSortedMapInterface<K, V> sample(double from_fraction,
			double to_fraction, String randSeed, long max_timestamp);

}