package ca.on.oicr.pde.deciders;

/**
 * Used to store IUS with its provenance object.
 *
 * @author mlaszloffy
 * @param <ProvenanceWithProvider>
 */
public class IusWithProvenance<ProvenanceWithProvider> {

    private final Integer iusSwid;
    private final ProvenanceWithProvider provenanceWithProvider;

    public IusWithProvenance(Integer iusSwid, ProvenanceWithProvider provenanceWithProvider) {
        this.iusSwid = iusSwid;
        this.provenanceWithProvider = provenanceWithProvider;
    }

    public Integer getIusSwid() {
        return iusSwid;
    }

    public ProvenanceWithProvider getProvenanceWithProvider() {
        return provenanceWithProvider;
    }

}
