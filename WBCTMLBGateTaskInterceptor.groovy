import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.UnitCategoryEnum
import com.navis.argo.business.reference.Equipment
import com.navis.external.road.AbstractGateTaskInterceptor
import com.navis.framework.business.Roastery
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageLevel
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.units.Unit
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.model.TruckingCompany
import com.navis.road.business.workflow.TransactionAndVisitHolder
import org.apache.log4j.Logger

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 29-Nov-2024
*
*  Requirements: WBCT- 72- Land Bridge Ops Trucking Company Validation
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: WBCTMLBGateTaskInterceptor
*     Code Extension Type: GATE_TASK_INTERCEPTOR
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTMLBGateTaskInterceptor extends AbstractGateTaskInterceptor {

    @Override
    void execute(TransactionAndVisitHolder inTran) {
        LOGGER.warn("WBCTMLBGateTaskInterceptor executing...")
        TruckTransaction truckTran = inTran.getTran()

        if (truckTran != null) {
            String ctrNbr = truckTran.getCtrNbrOrRequested()
            TruckingCompany trkCompany = truckTran.getTranTruckingCompany()
            if (ArgoUtils.isNotEmpty(ctrNbr) && trkCompany != null) {
                boolean isOffDockUnit = findOffDockUnit(ctrNbr)
                if (isOffDockUnit && (ArgoUtils.isEmpty(trkCompany.getTrkcRefId()) || !IS_MLB_CARRIER.equalsIgnoreCase(trkCompany.getTrkcRefId()))) {
                    getMessageCollector().appendMessage(MessageLevel.SEVERE, PropertyKeyFactory.valueOf("LAND_BRIDGE_OPS_FAILED"), null, trkCompany.getBzuId(), ctrNbr)                }
            }
        }
        executeInternal(inTran)
    }

    boolean findOffDockUnit(String ctr) {
        boolean isOffDockUnit = false
        Equipment equip = Equipment.findEquipment(ctr)
        if (equip != null) {
            Unit unit = unitFinder.findActiveUnit(ContextHelper.getThreadComplex(), equip, UnitCategoryEnum.IMPORT)
            if (unit != null && ArgoUtils.isNotEmpty(unit?.getUnitFlexString02()) && IS_OFF_DOCK.equals(unit.getUnitFlexString02())) {
                isOffDockUnit = true
            }
        }
        return isOffDockUnit
    }

    private static final String IS_MLB_CARRIER = "Y"
    private static final String IS_OFF_DOCK = "OFF"
    private static final UnitFinder unitFinder = (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID)
    private static final Logger LOGGER = Logger.getLogger(WBCTMLBGateTaskInterceptor.class);
}
