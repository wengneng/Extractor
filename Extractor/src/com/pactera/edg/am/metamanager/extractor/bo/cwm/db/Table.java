package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import java.util.ArrayList;
import java.util.List;

public class Table extends NamedColumnSet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1285721347550196627L;

	public final static String TABLE_NAME = "TABLE_NAME";

	public final static String TABLE_TYPE = "TABLE_TYPE";

	public static final String TABLESPACE_NAME = "tablespace_name";

	private List<Partition> partitions = new ArrayList<Partition>(0);

	private List<PrimaryKey> primaryKeies = new ArrayList<PrimaryKey>(0);

	private List<ForeignKey> foreignKeies = new ArrayList<ForeignKey>(0);

	public List<PrimaryKey> getPrimaryKeies() {
		return primaryKeies;
	}

	public void setPrimaryKeies(List<PrimaryKey> primaryKeies) {
		this.primaryKeies = primaryKeies;
	}

	public List<ForeignKey> getForeignKeies() {
		return foreignKeies;
	}

	public void setForeignKeies(List<ForeignKey> foreignKeies) {
		this.foreignKeies = foreignKeies;
	}

	public void addPrimaryKey(PrimaryKey pk) {
		primaryKeies.add(pk);

	}

	public void addForeignKey(ForeignKey fk) {
		foreignKeies.add(fk);

	}

	public List<Partition> getPartitions() {
		return partitions;
	}

	public void addPartition(Partition partition) {
		partitions.add(partition);
	}

	public int compareTo(NamedColumnSet o) {
		return super.getName().compareTo(o.getName());
	}

}
