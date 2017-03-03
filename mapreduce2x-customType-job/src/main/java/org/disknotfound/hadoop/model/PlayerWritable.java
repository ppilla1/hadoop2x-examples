package org.disknotfound.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

public class PlayerWritable implements WritableComparable<PlayerWritable> {
	private static final Logger LOG = Logger.getLogger(PlayerWritable.class);
	
	private IntWritable yearID = new IntWritable();
	private Text teamID = new Text();
	private Text lgID = new Text();
	private Text playerID= new Text();
	private LongWritable salary = new LongWritable();
	
	public PlayerWritable(){}
	
	public PlayerWritable(Player player){
		set(player);
	}
	
	public void set(Player player){
		this.yearID = new IntWritable(player.getYearID());
		this.teamID = new Text(player.getTeamID());
		this.lgID = new Text(player.getLgID());
		this.playerID = new Text(player.getPlayerID());
		this.salary = new LongWritable(player.getSalary());
	}
	
	public Player get(){
		Player player = new Player();
		
		player.setYearID(this.yearID.get())
			  .setTeamID(this.teamID.toString())
			  .setLgID(this.lgID.toString())
			  .setPlayerID(this.playerID.toString())
			  .setSalary(this.salary.get());
		
		return player;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		this.yearID.write(out);
		this.teamID.write(out);
		this.lgID.write(out);
		this.playerID.write(out);
		this.salary.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		 this.yearID.readFields(in);
		 LOG.info("YearID -> "+this.yearID.get());
		 this.teamID.readFields(in);
		 LOG.info("TeamID -> "+this.teamID.toString());
		 this.lgID.readFields(in);
		 LOG.info("lgID -> "+this.lgID.toString());
		 this.playerID.readFields(in);
		 LOG.info("PlayerID -> "+this.playerID.toString());
		 this.salary.readFields(in);
		 LOG.info("Salary -> "+this.salary.toString());
	}

	@Override
	public int compareTo(PlayerWritable o) {
		return o.salary.compareTo(this.salary);
	}

}
