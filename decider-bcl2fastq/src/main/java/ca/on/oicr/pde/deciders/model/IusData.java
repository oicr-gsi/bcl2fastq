package ca.on.oicr.pde.deciders.model;

import net.sourceforge.seqware.common.model.IUS;
import ca.on.oicr.ws.dto.SampleDto;

public class IusData {
	
	private IUS swIus;
	private SampleDto limsSample;
	
	public IusData() {
		
	}
	
	public IusData(IUS swIus, SampleDto limsSample) {
		this.swIus = swIus;
		this.setLimsSample(limsSample);
	}

	public IUS getSwIus() {
		return swIus;
	}

	public void setSwIus(IUS swIus) {
		this.swIus = swIus;
	}

	public SampleDto getLimsSample() {
		return limsSample;
	}

	public void setLimsSample(SampleDto limsSample) {
		this.limsSample = limsSample;
	}

}
