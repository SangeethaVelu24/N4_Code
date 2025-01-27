import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.model.CarrierVisit
import com.navis.external.road.AbstractGateTaskInterceptor
import com.navis.inventory.business.units.Unit
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.workflow.TransactionAndVisitHolder
import com.navis.vessel.business.schedule.VesselVisitDetails
import com.navis.vessel.business.schedule.VesselVisitLine
import org.apache.log4j.Logger

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 23-Sep-2024
*
*  Requirements: validate custom OOG cutoff date in gate
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: WBCTCustomRejectCarrierVisitPastCutoff
*     Code Extension Type: GATE_TASK_INTERCEPTOR
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTCustomRejectCarrierVisitPastCutoff extends AbstractGateTaskInterceptor {

    @Override
    void execute(TransactionAndVisitHolder inTran) {

        executeInternal(inTran)

        LOGGER.warn("WBCTCustomRejectCarrierVisitPastCutoff executing...")
        TruckTransaction truckTran = inTran.getTran()
        if (truckTran != null) {
            Unit unit = truckTran.getTranUnit()
            LOGGER.warn("unit " + unit)
            if (unit != null) {
                LOGGER.warn("unit.getUnitIsOog() " + unit.getUnitIsOog())
                if (unit.getUnitIsOog()) {
                    CarrierVisit visit = truckTran.getTranCarrierVisit()
                    String lineId = truckTran.getTranLineId()
                    if (visit != null && ArgoUtils.isNotEmpty(lineId)) {
                        VesselVisitDetails vvd = VesselVisitDetails.resolveVvdFromCv(visit)
                        if (vvd != null) {
                            Date oogLineDate = retrieveLineCutOff(vvd, lineId)
                            Date oogVesselDate = oogLineDate != null ? oogLineDate : (vvd.getVvFlexDate03() != null ? vvd.getVvFlexDate03() : null)
                            if (oogVesselDate != null) {
                                if (ArgoUtils.timeNow() > oogVesselDate) {
                                    registerError("Past OOG cutoff " + oogVesselDate)
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Date retrieveLineCutOff(VesselVisitDetails vvd, String lineId) {
        Date oogDate = null
        if (vvd != null && ArgoUtils.isNotEmpty(lineId)) {
            Set<VesselVisitLine> vesselVisitLineSet = (Set<VesselVisitLine>) vvd.getVvdVvlineSet()
            if (vesselVisitLineSet != null && vesselVisitLineSet.size() > 0) {
                for (VesselVisitLine vvl : vesselVisitLineSet) {
                    String vvLineId = vvl.getVvlineBizu()?.getBzuId()
                    if (ArgoUtils.isNotEmpty(vvLineId)) {
                        if (lineId.equals(vvLineId)) {
                            oogDate = vvl.getVvlineTimeActivateYard()
                        }
                    }
                }
            }
        }
        return oogDate
    }
    private static final Logger LOGGER = Logger.getLogger(WBCTCustomRejectCarrierVisitPastCutoff.class);

}
