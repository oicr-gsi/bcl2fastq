package ca.on.oicr.pde.deciders;

import ca.on.oicr.gsi.provenance.model.LimsKey;
import ca.on.oicr.gsi.provenance.model.LimsProvenance;
import net.sourceforge.seqware.common.dto.LimsKeyDto;

/**
 * Used to store provenance object with its provider.
 *
 * @author mlaszloffy
 * @param <T extends LimsProvenance>
 */
public class ProvenanceWithProvider<T extends LimsProvenance> {

    private final String provider;
    private final T provenance;

    public ProvenanceWithProvider(String provider, T provenance) {
        this.provider = provider;
        this.provenance = provenance;
    }

    public String getProvider() {
        return provider;
    }

    public T getProvenance() {
        return provenance;
    }

    public LimsKey getLimsKey() {
        LimsKeyDto lk = new LimsKeyDto();
        lk.setProvider(provider);
        lk.setId(provenance.getProvenanceId());
        lk.setLastModified(provenance.getLastModified());
        lk.setVersion(provenance.getVersion());
        return lk;
    }

}
