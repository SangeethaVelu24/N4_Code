package Caucedo

import com.navis.argo.business.reference.SpecialStow
import com.navis.external.road.AbstractGateTaskInterceptor
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.workflow.TransactionAndVisitHolder
import org.apache.log4j.Logger


/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha V; Date: 08/03/2024
*
*  Requirements: Update the special stow as XRAY, if the tranisXray
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: DPWCAUUpdateSpecialStow
*     Code Extension Type: GATE_TASK_INTERCEPTOR
*  4. Click Save button
*
*  @Setup:

 */

class DPWCAUUpdateSpecialStow extends AbstractGateTaskInterceptor {

    @Override
    void execute(TransactionAndVisitHolder inTran) {
        LOGGER.warn("DPWCAUUpdateSpecialStow executing....")
        if (inTran != null) {
            TruckTransaction truckTran = inTran.getTran()
            LOGGER.warn("truck tran " + truckTran)
            if (truckTran != null) {
                boolean isXrayRequired = truckTran.getTranIsXrayRequired()
                UnitFacilityVisit ufv = truckTran.getTranUfv()
                if (ufv != null) {
                    Unit unit = ufv.getUfvUnit()
                    if (unit != null) {
                        SpecialStow specialStow = SpecialStow.findSpecialStow("XRAY")
                        if (specialStow != null && isXrayRequired) {
                            unit.setUnitSpecialStow(specialStow)

                        }
                    }
                }
            }

        }
        executeInternal(inTran)
        //  super.execute(inTran)
    }
    private static final Logger LOGGER = Logger.getLogger(DPWCAUUpdateSpecialStow.class)

}
