package com.epam.bigdata.q3.task3.mapReduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Custom object for visits and spends.
 * 
 * @author Maryna_Maroz
 *
 */
public class VisitSpend implements WritableComparable<VisitSpend> {
	private int amountVisits;
	private int amountSpends;

	public VisitSpend() {
	}

	public VisitSpend(int visits, int spends) {
		this.amountVisits = visits;
		this.amountSpends = spends;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(amountVisits);
		out.writeInt(amountSpends);
	}

	public void readFields(DataInput in) throws IOException {
		amountVisits = in.readInt();
		amountSpends = in.readInt();
	}

	public int compareTo(VisitSpend vs) {
		if (Integer.compare(amountVisits, vs.getAmountVisits()) == 0) {
			return Integer.compare(amountSpends, vs.getAmountSpends());
		} else {
			return Integer.compare(amountVisits, vs.getAmountVisits());
		}
	}

	public int getAmountVisits() {
		return amountVisits;
	}

	public void setAmountVisits(int amountVisits) {
		this.amountVisits = amountVisits;
	}

	public int getAmountSpends() {
		return amountSpends;
	}

	public void setAmountSpends(int amountSpends) {
		this.amountSpends = amountSpends;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		VisitSpend other = (VisitSpend) obj;
		if (amountSpends != other.amountSpends)
			return false;
		if (amountVisits != other.amountVisits)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + amountSpends;
		result = prime * result + amountVisits;
		return result;
	}

	@Override
	public String toString() {
		return " Visits: " + amountVisits + "; Bidding price: " + amountSpends;
	}
}
