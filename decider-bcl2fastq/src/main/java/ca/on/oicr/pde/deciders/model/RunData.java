package ca.on.oicr.pde.deciders.model;

import java.util.Collection;

import net.sourceforge.seqware.common.model.SequencerRun;
import ca.on.oicr.ws.dto.RunDto;

public class RunData {

	private RunDto limsRun;
	private SequencerRun swRun;
	private Collection<RunPositionData> positions;
	
	public RunData() {
		
	}
	
	public RunData(RunDto limsRun, SequencerRun swRun, Collection<RunPositionData> positions) {
		this.limsRun = limsRun;
		this.swRun = swRun;
		this.positions = positions;
	}

	public RunDto getLimsRun() {
		return limsRun;
	}

	public void setLimsRun(RunDto limsRun) {
		this.limsRun = limsRun;
	}

	public SequencerRun getSwRun() {
		return swRun;
	}

	public void setSwRun(SequencerRun swRun) {
		this.swRun = swRun;
	}

	public Collection<RunPositionData> getPositions() {
		return positions;
	}

	public void setPositions(Collection<RunPositionData> positions) {
		this.positions = positions;
	}

}
