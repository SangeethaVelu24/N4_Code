package WBCT

import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.reference.LineOperator
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.road.business.model.TruckTransaction
import com.navis.vessel.business.schedule.VesselVisitDetails
import com.navis.vessel.business.schedule.VesselVisitLine

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 
*
*  Requirements: 
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: 
*     Code Extension Type: 
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTValidateVisitCutOffScriptRunner {

    String execute() {
        StringBuilder sb = new StringBuilder()
        long gkey = 370876615
        TruckTransaction tran = TruckTransaction.findTruckTransactionByGkey(gkey)
        sb.append("tran " + tran).append("\n")
        if (tran != null) {
            CarrierVisit visit = tran.getTranCarrierVisit()
            String lineOp = tran.getTranLine()?.getBzuId()
            sb.append("line op " + lineOp).append("\n")
            sb.append("carrier visit " + visit).append("\n")
            if (visit != null && lineOp != null) {
                VesselVisitDetails vvd = VesselVisitDetails.resolveVvdFromCv(visit)

                Set<VesselVisitLine> vesselVisitLineSet = (Set<VesselVisitLine>) vvd.getVvdVvlineSet()
                sb.append("vesselVisitLineSet " + vesselVisitLineSet).append("\n")
                if (vesselVisitLineSet != null && vesselVisitLineSet.size() > 0) {
                    for (VesselVisitLine vvl : vesselVisitLineSet) {
                        String lineId = vvl?.getVvlineBizu()?.getBzuId()
                        sb.append("line id " + lineId).append("\n")
                        if (ArgoUtils.isNotEmpty(lineOp) && ArgoUtils.isNotEmpty(lineId)) {
                            if (lineOp.equalsIgnoreCase(lineId)) {
                                Date oogDate = (Date) vvl.getFieldValue(oog_cut_off)
                                sb.append("oog date " + oogDate).append("\n")
                            }
                        }
                    }
                }

                sb.append("vvd " + vvd).append("\n")
                if (vvd != null) {
                    Date cutOffDate = vvd.getVvFlexDate03()
                    sb.append("cutOff date " + cutOffDate).append("\n")
                    if (ArgoUtils.timeNow() > cutOffDate) {
                        sb.append("past cut off date...")
                    }
                }
            }
        }

        return sb.toString()
    }
    private static final MetafieldId oog_cut_off = MetafieldIdFactory.valueOf("customFlexFields.vvlineCustomDFFOOG_CutOff");

}
