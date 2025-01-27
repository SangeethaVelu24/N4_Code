package WBCT


import com.navis.argo.business.api.IImpediment
import com.navis.argo.business.api.ServicesManager
import com.navis.argo.business.atoms.ChargeableUnitEventTypeEnum
import com.navis.argo.business.atoms.FlagStatusEnum
import com.navis.argo.business.atoms.LocTypeEnum
import com.navis.external.services.AbstractGeneralNoticeCodeExtension
import com.navis.framework.business.Roastery
import com.navis.inventory.business.api.UnitStorageManager
import com.navis.inventory.business.atoms.UfvTransitStateEnum
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.services.business.event.GroovyEvent
import org.apache.log4j.Logger

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 24-Dec-2024
*
*  Requirements: Checks the container is available for delivery then updates the unitFlexString07 as "Y"
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: WBCTCtrAvailableForDeliveryGeneralNotice
*     Code Extension Type: GENERAL_NOTICES_CODE_EXTENSION
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTCtrAvailableForDeliveryGeneralNotice extends AbstractGeneralNoticeCodeExtension {

    @Override
    void execute(GroovyEvent inGroovyEvent) {
        LOGGER.warn("WBCTCtrAvailableForDeliveryGeneralNotice executing...")
        Unit unit = (Unit) inGroovyEvent.getEntity()

        UnitFacilityVisit ufv = unit.getUnitActiveUfvNowActive()

        if (ufv != null && UfvTransitStateEnum.S40_YARD.equals(ufv.getUfvTransitState())
                && LocTypeEnum.TRUCK.equals(ufv?.getUfvObCv()?.getLocType())) {
            boolean isActive = isActiveFlag(unit)
            boolean isDemurrage = isDemurrageCharges(ufv)
            LOGGER.warn("isDemurrage " + isDemurrage)
            if (!isActive && !isDemurrage) {
                unit.setUnitFlexString07(DELIVERABLE)
            } else {
                unit.setUnitFlexString07(NON_DELIVERABLE)
            }
        }
        // super.execute(inGroovyEvent)
    }

    boolean isActiveFlag(Unit unit) {
        boolean isActive = false
        ServicesManager servicesManager = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID)
        List<IImpediment> impedimentList = servicesManager.getImpedimentsForEntity(unit)
        if (impedimentList != null && impedimentList.size() > 0) {
            for (IImpediment impediment : impedimentList) {
                if (FlagStatusEnum.ACTIVE.equals(impediment.getStatus())) {
                    isActive = true
                }
            }
        }

        return isActive
    }

    boolean isDemurrageCharges(UnitFacilityVisit ufv) {
        if (ufv) {
            UnitStorageManager storageManager = (UnitStorageManager) Roastery.getBean(UnitStorageManager.BEAN_ID)
            return storageManager.isStorageOwed(ufv, ChargeableUnitEventTypeEnum.STORAGE.getKey())
        }
    }

    private static final String DELIVERABLE = "Y"
    private static final String NON_DELIVERABLE = "N"
    private static final Logger LOGGER = Logger.getLogger(WBCTCtrAvailableForDeliveryGeneralNotice.class);

}
