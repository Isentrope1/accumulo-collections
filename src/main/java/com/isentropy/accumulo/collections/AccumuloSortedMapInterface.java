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