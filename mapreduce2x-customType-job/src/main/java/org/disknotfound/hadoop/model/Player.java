package org.disknotfound.hadoop.model;

public class Player {
	private int yearID;
	private String teamID;
	private String lgID;
	private String playerID;
	private long salary;
	
	public int getYearID() {
		return yearID;
	}
	public Player setYearID(int yearID) {
		this.yearID = yearID;
		return this;
	}
	public String getTeamID() {
		return teamID;
	}
	public Player setTeamID(String teamID) {
		this.teamID = teamID;
		return this;
	}
	public String getLgID() {
		return lgID;
	}
	public Player setLgID(String lgID) {
		this.lgID = lgID;
		return this;
	}
	public String getPlayerID() {
		return playerID;
	}
	public Player setPlayerID(String playerID) {
		this.playerID = playerID;
		return this;
	}
	public long getSalary() {
		return salary;
	}
	public Player setSalary(long salary) {
		this.salary = salary;
		return this;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((lgID == null) ? 0 : lgID.hashCode());
		result = prime * result + ((playerID == null) ? 0 : playerID.hashCode());
		result = prime * result + (int) (salary ^ (salary >>> 32));
		result = prime * result + ((teamID == null) ? 0 : teamID.hashCode());
		result = prime * result + yearID;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Player other = (Player) obj;
		if (lgID == null) {
			if (other.lgID != null)
				return false;
		} else if (!lgID.equals(other.lgID))
			return false;
		if (playerID == null) {
			if (other.playerID != null)
				return false;
		} else if (!playerID.equals(other.playerID))
			return false;
		if (salary != other.salary)
			return false;
		if (teamID == null) {
			if (other.teamID != null)
				return false;
		} else if (!teamID.equals(other.teamID))
			return false;
		if (yearID != other.yearID)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "Player [yearID=" + yearID + ", teamID=" + teamID + ", lgID=" + lgID + ", playerID=" + playerID
				+ ", salary=" + salary + "]";
	}
}