package WBCT

import com.navis.argo.ContextHelper
import com.navis.external.argo.AbstractGroovyJobCodeExtension
import com.navis.external.framework.util.ExtensionUtils
import com.navis.framework.persistence.hibernate.CarinaPersistenceCallback
import com.navis.framework.persistence.hibernate.PersistenceTemplate
import org.apache.log4j.Logger

/**
 * Extractor Job base code
 */
class WBCTExtractMastersGroovyJob extends AbstractGroovyJobCodeExtension {

    @Override
    void execute(Map<String, Object> inParams) {

        try {
            PersistenceTemplate pt = new PersistenceTemplate(getUserContext())
            pt.invoke(new CarinaPersistenceCallback() {
                protected void doInTransaction() {

                    listOfEntities.each {
                        entity ->
                            def extension = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "WBCT${entity}Extractor");
                            extension.execute()
                    }

                }
            })
        } catch (Exception e) {
            log.warn("Exception occurred while executing WBCTExtractMastersGroovyJob")
        }

    }

    private static final Logger log = Logger.getLogger(this.class)
    def listOfEntities = ['Vessel']
    /*def listOfEntities = ['Vessel', 'VesselVisit', 'TruckingCompanies', 'Vessel', 'ShipClass', 'Trucks',
                          'RailcarTypes', 'RailCars', 'TrainVisit', 'Accessory', 'CarrierService']*/

    //['ChassisUse','ContainerUse']
}
